import os
import uuid
import requests

from LoggingSetup import get_logger
from QonicAuth import login

logger = get_logger()

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
    def _handle_error(err: Exception):
        if isinstance(err, requests.HTTPError):
            response = err.response
            logger.error(f"HTTP error occurred: {response.status_code} - {response.text}")
            return
        else:
            logger.error(f"An error occurred: {err}")
        exit()

    def _get(self, path, params=None):
        try:
            response = requests.get(f"{self.api_url}{path}", params=params, headers=self._headers())
            response.raise_for_status()
            return response.json()
        except Exception as err:
            self._handle_error(err)

    def _post(self, path, data=None, json=None, params=None):
        try:
            response = requests.post(f"{self.api_url}{path}", data=data, json=json, params=params, headers=self._headers())
            response.raise_for_status()
            return response
        except Exception as err:
            self._handle_error(err)

    def _put(self, path, data=None, json=None, params=None):
        try:
            response = requests.put(f"{self.api_url}{path}", data=data, json=json, params=params, headers=self._headers())
            response.raise_for_status()
            return response
        except Exception as err:
            self._handle_error(err)

    def _delete(self, path, data=None, json=None, params=None):
        response = None
        try:
            response = requests.delete(f"{self.api_url}{path}", data=data, json=json, params=params, headers=self._headers())
            response.raise_for_status()
            return response
        except Exception as err:
            self._handle_error(err)

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

    def modify_model_data(self, project_id: str, model_id: str, modifications: dict):
        """
        Modify a model in Qonic using ExternalDataModification structure.

        Args:
            project_id (str): The ID of the project.
            model_id (str): The ID of the model.
            modifications (dict): A dictionary containing the modifications to be made.
                It should have the structure:
                {
                    "add": { "FieldName": { "Guid": "value" } },
                    "update": { "FieldName": { "Guid": "value" } },
                    "delete": { "FieldName": { "Guid": "value" } }
                }
        Returns:
            dict: The response from the Qonic API after applying modifications.
        """
        path = f"projects/{project_id}/models/{model_id}/products"
        response = self._post(path, json=modifications)

        return response.json()

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