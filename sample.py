import os
import json

from LoggingSetup import setup_logging, get_logger
setup_logging(log_dir="logs", base_name="qonic_maximo_sync.log")

from dotenv import load_dotenv; load_dotenv()
from AssetMapper import qonic_product_to_maximo_asset, get_valid_codes, ASSETSPEC_MAP, filter_products_by_code
from LocationMapper import qonic_product_to_maximo_functional_location


from QonicClient import QonicClient
from MaximoClient import MaximoClient

projectId = os.environ["QONIC_PROJECT_ID"]
modelId = os.environ["QONIC_MODEL_ID"]
# productFilters = {"Guid": "1wddXsDr17pA_ixjMwTDUA"}
# productFilters = { "Guid": "0ghk9ho2X6Pe20aU4np$1K" }
# productFilters = { "Guid": "3YxdsPhef6Tw3MjY2qVfXL" }
# productFilters = { "Guid": "1wddXsDr17pA_ixjMwTDH0" }
codeFilter = ['AHU', 'CHHEPU', 'COVAHV','FACOUN', 'FAN', 'FIDASMDA', 'FIFISYHV','FIFISYPL', 'FLMEDE', 'PUHV']
productFilters = {}

qonicClient = QonicClient()
maximoClient = MaximoClient()
orgId = "BRU-ORG"
siteId = "BRU"
systemId = "PRIMARY"
parentId = "BUILDINGS"
qonicOperation = "add"
logger = get_logger()

locations = qonicClient.list_locations(projectId)
available_data = qonicClient.available_fields(projectId, modelId)
products = qonicClient.query_products(projectId, modelId, fields=available_data, filters=productFilters)
products = filter_products_by_code(products, codeFilter)
product_location_ids = {product['SpatialLocation']['SpatialLocationId'] for product in products if 'SpatialLocation' in product and 'SpatialLocationId' in product['SpatialLocation'] and len(product['SpatialLocation']['SpatialLocationId']) > 0}


synced=list()
for location_id in product_location_ids:
    maximoClient.sync_location_with_parents(location_id, locations, siteid=siteId, orgid=orgId, system_id=systemId, parent_id=parentId, synced=synced)
    logger.info(f"Synced location {location_id} with parents in Maximo.")

synced_count = 0
qonicModifications = {
    qonicOperation: {"FunctionalLocationId": {}, "AssetId": {}},
}

synced_locations = set([tuple((loc['location'], loc['lochierarchy'][0]['parent'])) for loc in synced])
synced_assets = set()
for i, product in enumerate(products):
    functional_location = maximoClient.sync_location(qonic_product_to_maximo_functional_location(product, siteid=siteId, orgid=orgId, system_id=systemId, locations=locations))
    asset_payload = qonic_product_to_maximo_asset(product, functional_location, orgid=orgId, siteid=siteId)
    if not asset_payload:
        logger.warning(f"Skipping product {product['Guid']} due to a problem with asset mapping. Deleting functional location {functional_location['location']} in Maximo.")
        maximoClient.delete_location(functional_location['location'], siteid=siteId, orgid=orgId)
        continue

    response = maximoClient.sync_asset(asset_payload)
    assetnum = response[0]['_responsedata']['assetnum']

    qonicModifications[qonicOperation]["FunctionalLocationId"][product['Guid']] = {"PropertySet": "BAC", "Value": functional_location['location']}
    qonicModifications[qonicOperation]["AssetId"][product['Guid']] = {"PropertySet": "BAC", "Value": assetnum}
    synced_count += 1
    synced_locations.add((functional_location['location'], functional_location['lochierarchy'][0]['parent']))
    synced_assets.add((product['Guid'], assetnum))
    createdAssetId = response[0]['_responsedata']['assetuid']
    logger.info(f"Synced functional location {functional_location['location']} for product {product['Guid']} in Maximo.")
    logger.info(f"Created/updated asset {assetnum} for product {product['Guid']} in Maximo.")
    if i % 100 == 0:
        logger.info(f"Processed {i}/{len(products)} products, synced {synced_count} assets so far.")

response = qonicClient.modify_model_data(projectId, modelId, modifications=qonicModifications)
if 'errors' in response and response['errors']:
    for error in response['errors']:
        logger.error(f"Failed to push modification: {error}")
else:
    logger.info(
        f"Successfully pushed {len(qonicModifications[qonicOperation]['FunctionalLocationId'])} functional locations and {len(qonicModifications[qonicOperation]['AssetId'])} assets to Qonic.")


new_assets = set(synced_assets)
new_locations = set(tuple(item) for item in synced_locations)

output_path = "synced_data.json"
if os.path.exists(output_path):
    with open(output_path, "r", encoding="utf-8") as f:
        existing = json.load(f)
    existing_assets = set(tuple(item) for item in existing.get("synced_assets", []))
    existing_locations = set(tuple(item) for item in existing.get("synced_locations", []))
else:
    existing_assets = set()
    existing_locations = set()

merged_assets = sorted(existing_assets | new_assets)
merged_locations = sorted(existing_locations | new_locations)

output_data = {
    "synced_assets": merged_assets,
    "synced_locations": [list(item) for item in merged_locations],
}

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(output_data, f, ensure_ascii=False, indent=4)

logger.info(f"Merged {len(new_assets)} assets and {len(new_locations)} locations into {output_path}")