import os
from typing import Optional

import requests

from LocationMapper import qonic_spatial_location_to_maximo_location


class MaximoException(Exception):
    """
    Custom exception for Maximo API errors.
    Includes optional reason code and status code.
    """

    def __init__(self, message: str, reason_code: str = None, status_code: str = None):
        self.reason_code = reason_code
        self.status_code = status_code
        super().__init__(f"{message} (Reason: {reason_code}, Status: {status_code})")


class MaximoClient:
    def __init__(self, api_url: str = None, api_key: str = None):
        self.api_url = api_url or os.environ["MAXIMO_API_URL"]
        self.api_key = api_key or os.environ["MAXIMO_API_KEY"]
        self.session = requests.Session()
        self.session.headers.update({
            "lean": "1",
            "apikey": self.api_key,
            "Content-Type": "application/json",
            "properties": "*",
            "x-method-override": "BULK",
        })
        self.cookies = {}  # You can set this dynamically if needed

    def _with_default_params(self, params: Optional[dict] = None) -> dict:
        params = params or {}
        if "lean" not in params:
            params["lean"] = 1
        if "apikey" not in params:
            params["apikey"] = self.api_key
        return params

    @staticmethod
    def _check_maximo_response(response: requests.Response):
        """
        Validate HTTP status and Maximo JSON error payload.
        Raises MaximoException for Maximo-specific errors.
        Raises HTTPError for standard HTTP errors.
        Returns the parsed JSON response if no errors are found.
        """
        try:
            response.raise_for_status()
            json_data = response.json()
        except requests.HTTPError as http_err:
            raise MaximoException(f"HTTP error occurred: {http_err}") from http_err
        except ValueError:
            # Non-JSON response, just return text
            return response.text

        # Check for Maximo error structure
        error_obj = None
        if isinstance(json_data, list) and json_data and "_responsedata" in json_data[0]:
            error_obj = json_data[0]["_responsedata"].get("Error")
        elif isinstance(json_data, dict) and "Error" in json_data:
            error_obj = json_data.get("Error")

        if error_obj:
            raise MaximoException(
                message=error_obj.get("message", "Unknown Maximo error"),
                reason_code=error_obj.get("reasonCode"),
                status_code=error_obj.get("statusCode")
            )

        return json_data

    def _get(self, path, params=None):
        response = self.session.get(f"{self.api_url}{path}", params=self._with_default_params(params),
                                    cookies=self.cookies)
        return self._check_maximo_response(response)

    def _post(self, path, data=None, json=None, params=None):
        response = self.session.post(f"{self.api_url}{path}", data=data, json=json,
                                     params=self._with_default_params(params), cookies=self.cookies)
        return self._check_maximo_response(response)

    def _put(self, path, data=None, json=None, params=None):
        response = self.session.put(f"{self.api_url}{path}", data=data, json=json,
                                    params=self._with_default_params(params), cookies=self.cookies)
        return self._check_maximo_response(response)

    def _delete(self, path, data=None, json=None, params=None):
        response = self.session.delete(f"{self.api_url}{path}", data=data, json=json,
                                       params=self._with_default_params(params), cookies=self.cookies)
        return self._check_maximo_response(response)

    def _patch(self, path, patch_data):
        return self.session.post(f"{self.api_url}{path}", json=patch_data, headers={**self.session.headers, **{
            "Content-Type": "application/json", "x-method-override": "PATCH"}},
                                 cookies=self.cookies
                                 )

    def get_asset(self, asset_id: str):
        """Get an asset by ID."""
        return self._get(f"MXAPIASSET/{asset_id}")

    def query_assets(
            self,
            where: Optional[str] = None,
            select: Optional[str] = None,
            order_by: Optional[str] = None,
            page_size: int = 100,
            page: int = 1,
            include_count: bool = True,
            stable_paging: bool = True
    ):
        """
        Query paged assets with optional filters and field selection.

        Args:
            where (str): OSLC where clause (e.g., "status='OPERATING'")
            select (str): OSLC select clause (e.g., "assetnum,status,location")
            order_by (str): OSLC order by clause (e.g., "+assetnum")
            page_size (int): Number of records per page.
            page (int): Page index (starting from 1).
            include_count (bool): Whether to include total count in response.
            stable_paging (bool): Whether to enable stable paging (session-based).
        Returns:
            dict: JSON response with assets and optionally metadata.
        """
        params = {
            "lean": 1,
            "oslc.pageSize": page_size,
            "oslc.paging": "true",
            "collectioncount": int(include_count),
            "stablepaging": int(stable_paging)
        }

        if where:
            params["oslc.where"] = where
        if select:
            params["oslc.select"] = select
        if order_by:
            params["oslc.orderBy"] = order_by
        if page > 1:
            params["pageno"] = page

        return self._get("MXAPIASSET", params=params)

    def query_asset_classes(
            self,
            where: Optional[str] = None,
            select: Optional[str] = None,
            order_by: Optional[str] = None,
            page_size: int = 100,
            page: int = 1,
            include_count: bool = True,
            stable_paging: bool = True
    ):
        """
        Query MXAPITKCLASS (asset classes) with optional filters and field selection.

        Args:
            where (str): OSLC where clause (e.g., "classificationid='12345'")
            select (str): OSLC select clause (e.g., "classificationid,description")
            order_by (str): OSLC order by clause (e.g., "+classificationid")
            page_size (int): Number of records per page.
            page (int): Page index (starting from 1).
            include_count (bool): Whether to include total count in response.
            stable_paging (bool): Whether to enable stable paging (session-based).

        Returns:
            dict: JSON response with asset classes.
        """
        params = {
            "lean": 1,
            "oslc.pageSize": page_size,
            "oslc.paging": "true",
            "collectioncount": int(include_count),
            "stablepaging": int(stable_paging)
        }

        if where:
            params["oslc.where"] = where
        if select:
            params["oslc.select"] = select
        if order_by:
            params["oslc.orderBy"] = order_by
        if page > 1:
            params["pageno"] = page

        return self._get("mxapitkclass", params=params)

    def sync_asset(self, asset_data: dict):
        """
        Create or update an asset using AddChange bulk PATCH.

        Args:
            asset_data (dict): Asset fields, e.g., {
                "assetnum": "A123",
                "siteid": "BRU",
                "orgid": "MYORG",
                "moved": False,
                "changedate": "2025-07-16"
            }

        Returns:
            dict: Parsed JSON response from Maximo
        """
        payload = [
            {
                "_data": {
                    "_action": "AddChange",
                    **asset_data
                },
                "_meta": {
                    "method": "PATCH",
                    "patchtype": "MERGE"
                }
            }
        ]
        return self._post("MXAPIASSET", json=payload)

    def delete_asset(self, assetnum: str, siteid: str, orgid: str):
        """
        Delete an asset using the AddChange bulk PATCH method.

        Args:
            assetnum (str): Asset number to delete.
            siteid (str): Maximo site ID.
            orgid (str): Maximo organization ID.

        Returns:
            dict: Parsed JSON response from Maximo
        """
        payload = [
            {
                "_data": {
                    "_action": "Delete",
                    "assetnum": assetnum,
                    "siteid": siteid,
                    "orgid": orgid,
                },
                "_meta": {
                    "method": "PATCH",
                    "patchtype": "MERGE"
                }
            }
        ]
        return self._post("MXAPIASSET", json=payload)


    def get_domain_values(self, domain_id: str) -> list[str]:
        """
        Fetch valid values for a given domain.
        """
        response = self._get(
            "mxapidomain",
            params={
                "oslc.where": f'domainid="{domain_id}"',
                "oslc.select": "spi:alndomain"
            }
        )

        values: list[str] = []
        members = response.get("member", [])
        for member in members:
            alndomain_values = member.get("alndomain", [])
            for item in alndomain_values:
                val = item.get("value")
                if val:
                    values.append(val)
        return values

    def get_class_structure(self, classstructureid: str, property_code: str) -> Optional[str]:
        """
        Retrieve the domainid for a given property_code in a classstructure.
        """
        response = self._get("MXAPICLASSSTRUCTURE", params={
            "oslc.where": f'classstructureid="{classstructureid}",classificationid="{property_code}"',
            "oslc.select": "spi:classspec"
        })
        members = response.get("member", [])
        return members[0]['classspec'] if members else None

    def sync_location(self, location_data: dict):
        """
        Create or update a location using AddChange bulk PATCH.

        Args:
            location_data (dict): Location fields, e.g., {
                "location": "BRUCARGO002",
                "description": "Brucargo Warehouse",
                "siteid": "BRU",
                "orgid": "BRU-ORG",
                "type": "OPERATING",
                "status": "OPERATING",
                "langcode": "EN",
                "changeby": "ADMIN",
                "changedate": "2025-07-22",
                "statusdate": "2025-07-22",
                "autowogen": False
            }

        Returns:
            dict: Parsed JSON response from Maximo
        """
        payload = [
            {
                "_data": {
                    "_action": "AddChange",
                    **location_data
                },
                "_meta": {
                    "method": "PATCH",
                    "patchtype": "MERGE"
                }
            }
        ]

        response = self._post("QONIC_MXAPILOCATIONS", json=payload)
        return response[0]["_responsedata"] if response and '_responsedata' in response[0] else None

    def remove_location(self, location: str, siteid: str, orgid: str):
        """
        Remove the parent location from a given location in Maximo.

        Args:
            location_id (str): The Qonic location ID to update.
            siteid (str): Maximo site ID.
            orgid (str): Maximo organization ID.

        Returns:
            dict: Parsed JSON response from Maximo
        """
        payload = [
            {
                "_data": {
                    "_action": "Delete",
                    "location": location,
                    "siteid": siteid,
                    "orgid": orgid,
                },
                "_meta": {
                    "method": "PATCH",
                    "patchtype": "MERGE"
                }
            }
        ]

        response = self._post("QONIC_MXAPILOCATIONS", json=payload)
        return response[0]["_responsedata"] if response and '_responsedata' in response[0] else None

    def sync_location_with_parents(self, location_id, locations, siteid, orgid, system_id, parent_id, synced, delete=False):
        """
        Recursively ensure that all parent locations exist in Maximo before syncing the current location.

        Args:
            location_id (str): The Qonic location ID.
            locations (dict): Dictionary of all Qonic locations {guid: location_data}.
            siteid (str): Maximo site ID.
            orgid (str): Maximo organization ID.
            synced (set): A set of already synced locations to avoid duplicates.

        Returns:
            dict: The Maximo response of the current location.
        """
        # If location is already synced, skip
        if location_id in synced:
            return None

        if location_id not in locations:
            print(f"Location ID {location_id} not found in Qonic locations.")
            return None

        location = locations[location_id]
        parent_guid = location['parentGuid']
        parent_name = locations[parent_guid]['name'] if parent_guid and parent_guid in locations else None

        if delete:
            self.remove_location(location['name'], siteid, orgid)

        # Sync parent first
        if not parent_guid or parent_name == 'Default':
            parent = parent_id
        else:
            parent = \
            self.sync_location_with_parents(parent_guid, locations, siteid, orgid, system_id, parent_id, synced, delete=delete)[
                'location']

        # Convert Qonic location to Maximo format and sync
        maximo_location = qonic_spatial_location_to_maximo_location(location, parent=parent, siteid=siteid, orgid=orgid,
                                                                    system_id=system_id)
        response = self.sync_location(maximo_location)
        print(
            f"Synced location '{response.get('location', 'UNKNOWN')}' for Qonic location '{location_id} with parent '{parent}'.")

        synced.add(location_id)
        return response
