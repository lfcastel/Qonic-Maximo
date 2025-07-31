import os

from dotenv import load_dotenv
from AssetMapper import qonic_product_to_maximo_asset

load_dotenv()

from QonicClient import QonicClient
from MaximoClient import MaximoClient

projectId = os.environ["QONIC_PROJECT_ID"]
modelId = os.environ["QONIC_MODEL_ID"]
productFilters = {"Guid": "1wddXsDr17pA_ixjMwTDUA"}
# TODO: only use attributes in Maximo classification structure
# TODO: location hierarchy in Qonic
# TODO: some specifications aren't getting through into Maximo
# TODO: fake location for every single asset (functional location of asset/process code) - BV Location + 1/2/3...
# TODO/Future: Show system hierarchy from IFC into Maximo (Bv. plumbing distribution system)
# TODO: Removal from Maximo of assets that no longer exist in Qonic
# TODO: IFC id als asset attribute toevoegen (toevoegen aan alle classificaties) - investigate by louis
qonicClient = QonicClient()
maximoClient = MaximoClient()
orgId = "BRU-ORG"
siteId = "BRU"
systemId = "PRIMARY"
parentId = "BUILDINGS"
deleteFirst = True

locations = qonicClient.list_locations(projectId)
available_data = qonicClient.available_fields(projectId, modelId)
products = qonicClient.query_products(projectId, modelId, fields=available_data, filters=productFilters)
product_location_ids = {product['SpatialLocation']['SpatialLocationId'] for product in products if 'SpatialLocation' in product and 'SpatialLocationId' in product['SpatialLocation']}

print("-" * 100)
synced = set()
if deleteFirst:
    for product in products:
        print(f"Deleting asset {product['Guid']} from Maximo...")
        maximoClient.delete_asset(product['Guid'], orgid=orgId, siteid=siteId)

for location_id in product_location_ids:
    maximoClient.sync_location_with_parents(location_id, locations, siteid=siteId, orgid=orgId, system_id=systemId, parent_id=parentId, synced=synced, delete=deleteFirst)

print(f"Synced {len(synced)} locations from Qonic to Maximo.")
print("-" * 100)
for product in products:
    response = maximoClient.sync_asset(qonic_product_to_maximo_asset(product, orgid=orgId, siteid=siteId, locations=locations))

    if len(response) != 1 or '_responsedata' not in response[0]:
        print(f"Unexpected response length from Maximo API when creating asset:\n{response}")
        exit()

    if 'Error' in response[0]['_responsedata']:
        print(f"Error creating asset in Maximo: {response[0]['_responsedata']['Error']['message']}")
        exit()

    createdAssetId = response[0]['_responsedata']['assetuid']
    print(f"Created Maximo asset '{createdAssetId}' for Qonic product '{product['Guid']}'.")

print (f"Synced {len(products)} products from Qonic to Maximo.")