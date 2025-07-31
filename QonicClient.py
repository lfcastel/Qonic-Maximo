import os
import uuid
import requests
from QonicAuth import login


class ModificationInputError:
    def __init__(self, guid, field, error, description):
        self.guid = guid
        self.field = field
        self.error = error
        self.description = description

    def __str__(self):
        return f"{self.guid}: {self.field}: {self.error}: {self.description}"

    def __repr__(self):
        return self.__str__()


class QonicClient:
    def __init__(self, api_url: str = None, token=None):
        self.api_url = api_url or os.environ["QONIC_API_URL"]
        self.token = token or login()
        self.session_id = str(uuid.uuid4())

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token.access_token}",
            "X-Client-Session-Id": self.session_id,
        }

    @staticmethod
    def _handle_error(response: requests.Response):
        try:
            print(response.json())
        except Exception as err:
            print(f"Error occurred while processing error response: {err}")

    def _get(self, path, params=None):
        response = None
        try:
            response = requests.get(f"{self.api_url}{path}", params=params, headers=self._headers())
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            self._handle_error(response)
            exit()
        except Exception as err:
            print(f"Other error occurred: {err}")
            exit()

    def _post(self, path, data=None, json=None, params=None):
        response = None
        try:
            response = requests.post(f"{self.api_url}{path}", data=data, json=json, params=params, headers=self._headers())
            response.raise_for_status()
            return response
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            self._handle_error(response)
            exit()
        except Exception as err:
            print(f"Other error occurred: {err}")
            exit()

    def _put(self, path, data=None, json=None, params=None):
        response = None
        try:
            response = requests.put(f"{self.api_url}{path}", data=data, json=json, params=params, headers=self._headers())
            response.raise_for_status()
            return response
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            self._handle_error(response)
            exit()
        except Exception as err:
            print(f"Other error occurred: {err}")
            exit()

    def _delete(self, path, data=None, json=None, params=None):
        response = None
        try:
            response = requests.delete(f"{self.api_url}{path}", data=data, json=json, params=params, headers=self._headers())
            response.raise_for_status()
            return response
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            self._handle_error(response)
            exit()
        except Exception as err:
            print(f"Other error occurred: {err}")
            exit()

    def available_fields(self, project_id, model_id):
        response = self._get(f"projects/{project_id}/models/{model_id}/products/available-data")
        return response["fields"]

    def query_products(self, project_id, model_id, fields=None, filters=None):
        payload = {}
        if fields:
            payload["fields"] = fields
        if filters:
            payload["filters"] = filters

        response = self._post(f"projects/{project_id}/models/{model_id}/products/query", json=payload)
        return response.json()["result"]

    def list_locations(self, project_id):
        """
        Retrieve a list of all spatial locations for the given project.
        """
        response = self._get(f"projects/{project_id}/locations")
        return build_guid_map(response.get("locationViews", []))

def get_guid(location):
    """Extract the Guid property from a location object."""
    for prop in location.get('properties', []):
        if prop.get('name') == 'Guid':
            return prop.get('value')
    return None

def build_guid_map(locations, guid_map=None, parent_guid=None):
    """
    Recursively build a map from GUID to location object.
    """
    if guid_map is None:
        guid_map = {}

    for loc in locations:
        loc['parentGuid'] = parent_guid if parent_guid else None
        guid = get_guid(loc)

        if guid:
            guid_map[guid] = loc

        children = loc.get('children', [])
        if children:
            build_guid_map(children, guid_map, guid)

    return guid_map