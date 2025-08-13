import random


def qonic_product_to_maximo_functional_location(product: dict, siteid: str, orgid: str, system_id: str, locations: dict) -> dict:
    """
    Convert a Qonic product into a Maximo functional location record payload (BAC Maximo POST format).
    Args:
        product (dict): Qonic product structure.
        siteid (str): Maximo Site ID.
        orgid (str): Maximo Org ID.
        system_id (str): System ID for the location hierarchy.

    Returns:
        dict: Payload ready to post to BAC Maximo API.
    """

    functional_location_id = product.get("FunctionalLocationId", {}).get('Value') or random.randint(10**7, 10**8 - 1)
    parent_guid = product.get('SpatialLocation', {}).get('SpatialLocationId')
    if parent_guid not in locations:
        raise ValueError(f"Parent location with GUID {parent_guid} not found in provided locations.")

    parent = locations[parent_guid]["name"].strip()
    return {
        "_action": "AddChange",
        "location": functional_location_id,
        "siteid": siteid,
        "orgid": orgid,
        "type": "OPERATING",
        "children": False,
        "systemid": system_id,
        "b_qrcode": functional_location_id,
        "lochierarchy": [
            {
                "parent": parent,
                "systemid": system_id
            }
        ]
    }

def qonic_spatial_location_to_maximo_location(qonic_location: dict, parent: str, siteid: str, orgid: str,
                                              system_id: str) -> dict:
    """
    Convert a Qonic spatial location into a Maximo location record payload (BAC Maximo POST format).

    Args:
        qonic_location (dict): Qonic spatial structure.
        parent (str): Maximo parent location name.
        siteid (str): Maximo Site ID.
        orgid (str): Maximo Org ID.
        system_id (str): System ID for the location hierarchy.

    Returns:
        dict: Payload ready to post to BAC Maximo API.
    """
    name = qonic_location.get("name")
    if not name:
        raise ValueError("Qonic location must have a 'name' property.")

    description = next((p["value"] for p in qonic_location.get("properties", []) if p.get("name") == "LongName"), None)
    has_children = bool(qonic_location.get("children"))

    return {
        "_action": "AddChange",
        "location": name,
        "description": description,
        "siteid": siteid,
        "orgid": orgid,
        "type": "OPERATING",
        "children": has_children,
        "systemid": system_id,
        "lochierarchy": [
            {
                "parent": parent,
                "systemid": system_id
            }
        ]
    }
