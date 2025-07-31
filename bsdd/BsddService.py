import json

import urllib.parse
from pathlib import Path

import requests
import urllib.parse
import time


BSDD_MAPPING_PATH = Path("bsdd/BsddMapping.json")

class BsddService:
    BASE_URL = "https://api.bsdd.buildingsmart.org/api/"

    def __init__(self, max_retries=100, rate_limit_wait=2):
        self.session = requests.Session()
        self.max_retries = max_retries
        self.rate_limit_wait = rate_limit_wait  # default wait if Retry-After is missing

    def _get(self, endpoint: str, params: dict = None):
        """Internal GET method with retry on 429 (rate limit)."""
        url = urllib.parse.urljoin(self.BASE_URL, endpoint)
        retries = 0
        while retries < self.max_retries:
            response = self.session.get(url, params=params)
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", self.rate_limit_wait))
                print(f"Rate limit hit. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
                continue
            if response.status_code != 200:
                raise Exception(f"Failed request: {response.status_code} - {response.text}")
            return response.json()
        raise Exception(f"Max retries reached for {url}")

    def get_classes(self, uri: str, use_nested_classes: bool = False, offset: int = 0, limit: int = 1000):
        """Fetch classes for a dictionary."""
        params = {
            "uri": uri,
            "useNestedClasses": str(use_nested_classes).lower(),
            "offset": offset,
            "limit": limit
        }
        return self._get("Dictionary/v1/Classes/", params)

    def get_class(self, uri: str, include_class_properties: bool = False):
        """Fetch details of a class including its properties."""
        params = {
            "uri": uri,
            "includeClassProperties": str(include_class_properties).lower()
        }
        return self._get("Class/v1/", params)

    def get_all_classes(self, bsdd_uri: str):
        """Fetch all classes of a BSDD dictionary."""
        data = self.get_classes(bsdd_uri)
        all_classes = data.get("classes", [])
        total_count = data.get("classesTotalCount", len(all_classes))
        offset = data.get("classesCount", len(all_classes))

        while offset < total_count:
            response = self.get_classes(bsdd_uri, offset=offset)
            count = response.get("classesCount", 0)
            if count == 0:
                break
            offset += count
            all_classes.extend(response.get("classes", []))

        return all_classes

    def get_all_classes_with_properties(self, bsdd_uri: str):
        """
        Fetch all classes and their properties for a given BSDD dictionary.
        Returns a dict:
        {
            "Class Name": { "Property Name": "Property Code", ... },
            ...
        }
        """
        all_classes = self.get_all_classes(bsdd_uri)
        result = {}

        for c in all_classes:
            class_code = c.get("code")
            class_uri = c.get("uri")

            if not class_code or not class_uri:
                continue

            class_details = self.get_class(class_uri, include_class_properties=True)
            properties = class_details.get("classProperties", [])

            result[class_code] = {}
            for p in properties:
                property_code = p.get("propertyCode")
                property_name = p.get("name")
                if not property_code or not property_name:
                    continue

                result[class_code][property_code] = {
                    'code': property_code,
                    'name': property_name,
                    'type': p.get('dataType', 'String'),
                }

            print(f"Fetched {len(properties)} properties for class: {class_code}")
            time.sleep(0.2)

        return result

def save_mapping(mapping):
    with BSDD_MAPPING_PATH.open("w", encoding="utf-8") as jf:
        json.dump(mapping, jf, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    bac_uri = "https://identifier.buildingsmart.org/uri/bac/BAC_OTL/0.1"
    service = BsddService()
    mapping = service.get_all_classes_with_properties(bac_uri)
    save_mapping(mapping)
    print(f"Saved {len(mapping)} classes with properties to {BSDD_MAPPING_PATH}")