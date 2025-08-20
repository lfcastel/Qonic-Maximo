from pathlib import Path
import json
from typing import Dict, List, TypedDict, Any, Optional
from datetime import datetime

from LoggingSetup import get_logger
from MaximoClient import MaximoClient

maximoClient = MaximoClient()
BSDD_MAPPING_PATH = Path("bsdd/BsddMapping.json")
logger = get_logger()


class AssetProperty(TypedDict):
    propertyCode: str
    dataType: str
    units: List[str]


ASSETSPEC_MAP: Dict[str, Dict[str, AssetProperty]] = {}
if BSDD_MAPPING_PATH.exists():
    with open(BSDD_MAPPING_PATH, "r", encoding="utf-8") as f:
        ASSETSPEC_MAP = json.load(f)

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
            logger.warning(f"Value '{value}' for property '{property_code}' is not in domain '{domainid}'.")
            return {"alnvalue": None}

    if data_type == "Boolean":
        return {"alnvalue": "Yes" if str(value).lower() in ["true", "yes", "y", "1"] else "No"}

    if data_type == "Integer":
        try:
            return {"numvalue": int(float(value))}
        except (ValueError, TypeError):
            logger.warning(f"Invalid Integer value '{value}' for property '{property_code}'.")
            return {"numvalue": None}

    if data_type == "Real":
        try:
            return {"numvalue": float(value)}
        except (ValueError, TypeError):
            logger.warning(f"Invalid Real value '{value}' for property '{property_code}'.")
            return {"numvalue": None}

    if data_type == "Time":
        if isinstance(value, datetime):
            return {"datevalue": value.isoformat()}
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                return {"datevalue": dt.isoformat()}
            except ValueError:
                logger.warning(f"Invalid date string '{value}' for property '{property_code}'.")
                return {"datevalue": None}

    return {"alnvalue": str(value)}

def build_assetspec_from_qonic(
        product: dict,
        code: str,
        classstructureid: str,
        orgid: str,
) -> list[dict]:
    if not code:
        return []

    class_lookup = ASSETSPEC_MAP.get(code, {})
    class_structure = maximoClient.get_class_structure(classstructureid, code)
    rows = []

    for attribute in class_structure:
        attrid = attribute.get("assetattrid")
        domainid = attribute.get('domainid') if attribute else None

        if attrid not in class_lookup:
            logger.warning(f"Attribute '{attrid}' not found in ASSETSPEC_MAP for Classification '{code}'")
            continue

        property = class_lookup[attrid]
        property_name = property.get("name")
        data_type = property.get("type", "String")
        if property_name not in product:
            logger.warning(f"Property '{property_name}' not found in product for class '{code}'")
            value_field = {}
        else:
            raw = prop_val(product[property_name])
            value_field = convert_value_for_maximo_field(raw, data_type, attrid, domainid)

        if not value_field:
            logger.warning(f"Invalid value for property '{property_name}' with for class '{code}'")
            value_field = {}

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
    codes = get_valid_codes(product)
    if not codes or len(codes) != 1:
        logger.warning(f"Product {product.get('Guid', 'Unknown')} does not have a valid code or has multiple codes: {codes}")
        return None, None, None

    code_value = codes[0]
    response = maximoClient.query_asset_classes(
        where=f'spi:classificationid="{code_value}"',
        select="spi:classstructureid,spi:hierarchypath"
    )

    members = response.get('member') or []
    if not members:
        logger.warning(f"No class structure found for code '{code_value}'")
        return code_value, None, None

    asset_class = members[0]
    return code_value, asset_class.get("classstructureid"), asset_class.get("hierarchypath")


def qonic_product_to_maximo_asset(product: dict, functional_location: dict, orgid: str, siteid: str) -> dict | None:
    """
    Convert a Qonic product dict to a Maximo AddChange asset format.
    """

    assetnum = product.get("AssetId", {}).get("Value")
    desc = product.get("Name") or prop_val(product.get("Description")) or assetnum
    manufacturer = prop_val(product.get("Manufacturer"))
    assettag = prop_val(product.get("Tag"))

    code, classstructureid, hierarchypath = get_asset_class_info(product)
    if not code or not classstructureid:
        return None

    functional_location_id = functional_location["location"]

    if len(desc) > 100:
        logger.warning(f"Description for asset {assetnum} is too long, truncating to 100 characters.")
        desc = desc[:100]

    asset = {
        "siteid": siteid,
        "orgid": orgid,
        "description": desc,
        "hierarchypath": hierarchypath,
        "location": functional_location_id,
        "b_qrcode": functional_location_id,
        "bim_ifcguid": product.get("Guid"),
    }

    if manufacturer:
        asset["manufacturer"] = manufacturer

    if assettag:
        asset["assettag"] = str(assettag)

    if assetnum:
        asset["assetnum"] = str(assetnum)

    if not classstructureid or not code:
        return asset

    assetspec_rows = build_assetspec_from_qonic(product, code, classstructureid, orgid)
    if assetspec_rows:
        asset["assetspec"] = assetspec_rows

    return asset

def get_valid_codes(product: dict) -> List[str]:
    codes = product.get("Code", {})
    return [code['Identification'] for code in codes.values() if 'Identification' in code and code['Identification'] in ASSETSPEC_MAP]

def filter_products_by_code(products: List[dict], codes: List[str]) -> List[dict]:
    """
    Filter products by a list of valid codes.

    Args:
        products (List[dict]): List of product dictionaries.
        codes (List[str]): List of valid codes to filter by.

    Returns:
        List[dict]: Filtered list of products that match any of the valid codes.
    """
    return [product for product in products if get_valid_codes(product) and any(code in get_valid_codes(product) for code in codes)]
