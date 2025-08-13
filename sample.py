import os

from LoggingSetup import setup_logging, get_logger
setup_logging(log_dir="logs", base_name="qonic_maximo_sync.log")

from dotenv import load_dotenv
from AssetMapper import qonic_product_to_maximo_asset
from LocationMapper import qonic_product_to_maximo_functional_location

load_dotenv()

from QonicClient import QonicClient
from MaximoClient import MaximoClient

# TODO: Removal from Maximo of assets that no longer exist in Qonic
# TODO: IFC id als asset attribute toevoegen (toevoegen aan alle classificaties) - investigate by louis

projectId = os.environ["QONIC_PROJECT_ID"]
modelId = os.environ["QONIC_MODEL_ID"]
# productFilters = {"Guid": "1wddXsDr17pA_ixjMwTDUA"}
# productFilters = { "Guid": "0ghk9ho2X6Pe20aU4np$1K" }
# productFilters = { "Guid": "3YxdsPhef6Tw3MjY2qVfXL" }
# productFilters = { "Guid": "1wddXsDr17pA_ixjMwTDH0" }
productFilters = {}

qonicClient = QonicClient()
maximoClient = MaximoClient()
orgId = "BRU-ORG"
siteId = "BRU"
systemId = "PRIMARY"
parentId = "BUILDINGS"
logger = get_logger()
deleteFirst = False

locations = qonicClient.list_locations(projectId)
available_data = qonicClient.available_fields(projectId, modelId)

products = qonicClient.query_products(projectId, modelId, fields=available_data, filters=productFilters)
product_location_ids = {product['SpatialLocation']['SpatialLocationId'] for product in products if 'SpatialLocation' in product and 'SpatialLocationId' in product['SpatialLocation']}


synced = set()
if deleteFirst:
    for product in products:
        maximoClient.delete_asset(product['Guid'], orgid=orgId, siteid=siteId)
        logger.info(f"Deleted asset {product['Guid']} from Maximo.")

for location_id in product_location_ids:
    maximoClient.sync_location_with_parents(location_id, locations, siteid=siteId, orgid=orgId, system_id=systemId, parent_id=parentId, synced=synced, delete=deleteFirst)
    logger.info(f"Synced location {location_id} with parents in Maximo.")

synced_count = 0
qonicModifications = {
    "add": {"FunctionalLocationId": {}}
}

for product in products:
    functional_location = maximoClient.sync_location(qonic_product_to_maximo_functional_location(product, siteid=siteId, orgid=orgId, system_id=systemId, locations=locations))
    logger.info(f"Synced functional location {functional_location['location']} for product {product['Guid']} in Maximo.")

    qonicModifications["add"]["FunctionalLocationId"][product['Guid']] = {"PropertySet": "",
                                                                          "Value": functional_location['location']}
    asset_payload = qonic_product_to_maximo_asset(product, functional_location, orgid=orgId, siteid=siteId)
    if not asset_payload:
        logger.warning(f"Skipping product {product['Guid']} due to a problem with asset mapping.")
        continue

    response = maximoClient.sync_asset(asset_payload)

    createdAssetId = response[0]['_responsedata']['assetuid']
    synced_count += 1
    logger.info(f"Created/updated asset {createdAssetId} for product {product['Guid']} in Maximo.")

response = qonicClient.modify_model_data(projectId, modelId, modifications=qonicModifications)
if 'errors' in response and response['errors']:
    for error in response['errors']:
        logger.error(f"Failed to push modification: {error}")
else:
    logger.info(f"Successfully pushed {len(qonicModifications['add']['FunctionalLocationId'])} functional locations to Qonic.")
