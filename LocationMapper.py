def qonic_spatial_location_to_maximo_location(qonic_location: dict, parent: str, siteid: str, orgid: str,
                                              system_id: str) -> dict:
    """
    Convert a Qonic spatial location into a Maximo location record payload (BAC Maximo POST format).

    Args:
        qonic_location (dict): Qonic spatial structure.
        parent (str): Maximo parent location name.
        siteid (str): Maximo Site ID.
        orgid (str): Maximo Org ID.

    Returns:
        dict: Payload ready to post to BAC Maximo API.
    """
    name = qonic_location.get("name")
    if not name:
        raise ValueError("Qonic location must have a 'name' property.")

    guid = next((p["value"] for p in qonic_location.get("properties", []) if p.get("name") == "Guid"), None)
    description = f"{name} (GUID: {guid})" if guid else name
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
