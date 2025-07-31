from pathlib import Path
import json
from typing import Dict, List, TypedDict, Any, Optional
from datetime import datetime

from MaximoClient import MaximoClient

maximoClient = MaximoClient()
BSDD_MAPPING_PATH = Path("bsdd/BsddMapping.json")


class AssetProperty(TypedDict):
    propertyCode: str
    dataType: str
    units: List[str]


ASSETSPEC_MAP: Dict[str, Dict[str, AssetProperty]] = {}
if BSDD_MAPPING_PATH.exists():
    with open(BSDD_MAPPING_PATH, "r", encoding="utf-8") as f:
        ASSETSPEC_MAP = json.load(f)
        print(f"Loaded {len(ASSETSPEC_MAP)} mappings from {BSDD_MAPPING_PATH}")


def prop_val(obj):
    """
    Extract the Value from QonicProperty-like dicts: {'PropertySet': '...', 'Value': '...'}.
    If obj is already a str/num/bool, return it. Empty string -> None.
    """
    if isinstance(obj, dict):
        v = obj.get("Value", obj.get("value"))
    else:
        v = obj
    if v == "" or v is None:
        return None

    # TODO: A bug in Qonic sometimes adds extra whitespace around values
    return v.strip() if isinstance(v, str) else v



QONIC_TO_MAXIMO_TYPE = {
    "String": "aln",
    "Integer": "num",
    "Real": "num",
    "Boolean": "aln",
    "Time": "date"
}

def convert_value_for_maximo_field(
    value: Any,
    data_type: str,
    property_code: str,
    domainid: Optional[str] = None
) -> dict:
    """
    Convert and validate a Qonic value against Maximo's type and domain.
    """
    if domainid:
        valid_values = maximoClient.get_domain_values(domainid)
        if str(value) not in valid_values:
            print(f"Value '{value}' for property '{property_code}' is not in domain '{domainid}'.")
            return {"alnvalue": None}

    if data_type == "Boolean":
        return {"alnvalue": "Yes" if str(value).lower() in ["true", "yes", "y", "1"] else "No"}

    if data_type == "Integer":
        try:
            return {"numvalue": int(float(value))}
        except (ValueError, TypeError):
            print(f"Invalid Integer value '{value}' for property '{property_code}'.")
            return {"numvalue": None}

    if data_type == "Real":
        try:
            return {"numvalue": float(value)}
        except (ValueError, TypeError):
            print(f"Invalid Real value '{value}' for property '{property_code}'.")
            return {"numvalue": None}

    if data_type == "Time":
        if isinstance(value, datetime):
            return {"datevalue": value.isoformat()}
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                return {"datevalue": dt.isoformat()}
            except ValueError:
                print(f"Invalid date string '{value}' for property '{property_code}'.")
                return {"datevalue": None}

    # Default to string
    return {"alnvalue": str(value)}

def build_assetspec_from_qonic(
        product: dict,
        code: str,
        classstructureid: str,
        orgid: str,
) -> list[dict]:
    if not code:
        return []

    rows = []
    class_lookup = ASSETSPEC_MAP.get(code, {})

    class_structure = maximoClient.get_class_structure(classstructureid, code)
    for attribute in class_structure:
        attrid = attribute.get("assetattrid")
        domainid = attribute.get('domainid') if attribute else None

        if attrid not in class_lookup:
            print(f"Attribute '{attrid}' not found as property in BSDD for Classification '{code}'") # TODO error warning
            continue

        property = class_lookup[attrid]
        property_name = property.get("name")
        data_type = property.get("type", "String")
        raw = prop_val(product[property_name])
        value_field = convert_value_for_maximo_field(raw, data_type, attrid, domainid)
        if not value_field:
            print(f"Skipping property '{property_name}' with value '{raw}' for class '{code}'")
            continue

        row = {
            "classstructureid": classstructureid,
            "orgid": orgid,
            "assetattrid": attrid,
            "linearassetspecid": 0,
            **value_field
        }
        rows.append(row)

    return rows

def get_asset_class_info(product: dict) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extracts the code, classstructureid and hierarchypath for a given product.

    Args:
        product (dict): Product dictionary containing 'Code'.

    Returns:
        tuple: (code, classstructureid, hierarchypath) or (None, None, None) if not found.
    """
    code = product.get('Code', '').strip()
    code_value = code.split()[0] if code else None

    if not code_value:
        return None, None, None

    response = maximoClient.query_asset_classes(
        where=f'spi:classificationid="{code_value}"',
        select="spi:classstructureid,spi:hierarchypath"
    )

    members = response.get('member') or []
    if not members:
        return code_value, None, None

    asset_class = members[0]
    return code_value, asset_class.get("classstructureid"), asset_class.get("hierarchypath")


def qonic_product_to_maximo_asset(product: dict, *, orgid: str, siteid: str, locations: dict) -> dict:
    """
    Convert a Qonic product dict to a Maximo AddChange asset format.
    """

    assetnum = product.get("Guid")
    desc = product.get("Name") or prop_val(product.get("Description")) or assetnum
    manufacturer = prop_val(product.get("Manufacturer"))
    assettag = prop_val(product.get("Tag"))

    location_id = product.get('SpatialLocation', {}).get('SpatialLocationId')
    location_name = product.get('SpatialLocation', {}).get('name')
    if location_id and location_id in locations:
        location_name = locations[location_id].get('name')

    code, classstructureid, hierarchypath = get_asset_class_info(product)

    asset = {
        "assetnum": assetnum,
        "newassetnum": assetnum,
        "siteid": siteid,
        "orgid": orgid,
        "description": desc,
        "hierarchypath": hierarchypath,
        "location": location_name if location_name else None,
    }

    if manufacturer:
        asset["manufacturer"] = manufacturer

    if assettag:
        asset["assettag"] = str(assettag)

    if location_name:
        asset["location"] = location_name

    if not classstructureid or not code:
        return asset

    assetspec_rows = build_assetspec_from_qonic(product, code, classstructureid, orgid)
    if assetspec_rows:
        asset["assetspec"] = assetspec_rows

    return asset

