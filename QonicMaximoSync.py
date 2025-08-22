from collections import defaultdict

from AssetMapper import filter_products_by_code, qonic_product_to_maximo_asset
from LocationMapper import qonic_product_to_maximo_functional_location
from LoggingSetup import get_logger
from MaximoClient import MaximoClient, MaximoException
from ProgressTracker import get_progress_tracker
from QonicClient import QonicClient
import json

class QonicData:
    def __init__(self, locations: dict, products: list):
        self.locations = locations
        self.products = products
        self.production_location_ids = {product['SpatialLocation']['SpatialLocationId'] for product in products if
                                        'SpatialLocation' in product and 'SpatialLocationId' in product[
                                            'SpatialLocation'] and len(product['SpatialLocation']['SpatialLocationId']) > 0}

class QonicMaximoSync:
    def __init__(self, project_id: str, model_id: str):
        self.qonicClient = QonicClient()
        self.maximoClient = MaximoClient()
        self.progressService = get_progress_tracker()
        self.projectId = project_id
        self.modelId = model_id
        self.orgId = "BRU-ORG"
        self.siteId = "BRU"
        self.systemId = "PRIMARY"
        self.parentId = "BUILDINGS"
        self.logger = get_logger()
        self.qonic_data = None
        self.synced_locations = set()
        self.synced_assets = set()
        self.qonicOperation = "update"
        self.qonicModifications = {
            self.qonicOperation: {"FunctionalLocationId": {}, "AssetId": {}},
        }

    def init_qonic_data(self, product_filters: dict, code_filter: list) -> QonicData:
        locations = self.qonicClient.list_locations(self.projectId)
        available_data = self.qonicClient.available_fields(self.projectId, self.modelId)
        products = self.qonicClient.query_products(self.projectId, self.modelId, fields=available_data, filters=product_filters)
        products = filter_products_by_code(products, code_filter)
        self.qonic_data = QonicData(locations, products)
        return self.qonic_data

    def sync_locations(self):
        synced = list()
        for location_id in self.qonic_data.production_location_ids:
            self.sync_location(location_id, synced=synced)

    def sync_products(self):
        for i, product in enumerate(self.qonic_data.products):
            self.sync_product(product)
            if i % 100 == 0:
                self.logger.info(f"Processed {i}/{len(self.qonic_data.products)} products, synced {len(self.synced_assets)} assets so far.")

    def sync_location(self, location_id: str, synced: list):
        try:
            location = self.maximoClient.sync_location_with_parents(location_id, self.qonic_data.locations, siteid=self.siteId, orgid=self.orgId, system_id=self.systemId, parent_id=self.parentId, synced=synced)
            self.logger.info(f"Synced location {location_id} with parents in Maximo.")
            return location
        except Exception as e:
            self.logger.error(f"Failed to sync location {location_id}: {e}")
            return None

    def sync_product(self, product: dict):
        try:
            functional_location = self.maximoClient.sync_location(qonic_product_to_maximo_functional_location(product, siteid=self.siteId, orgid=self.orgId,
                                                            system_id=self.systemId,
                                                            locations=self.qonic_data.locations))
            self.progressService.add_location(functional_location['location'], functional_location['lochierarchy'][0]['parent'])
            asset_payload = qonic_product_to_maximo_asset(product, functional_location, orgid=self.orgId, siteid=self.siteId)
            if not asset_payload:
                self.logger.warning(
                    f"Skipping product {product['Guid']} due to a problem with asset mapping. Deleting functional location {functional_location['location']} in Maximo.")
                self.maximoClient.delete_location(functional_location['location'], siteid=self.siteId, orgid=self.orgId)
                self.progressService.delete_location(functional_location['location'], functional_location['lochierarchy'][0]['parent'])
                return None

            response = self.maximoClient.sync_asset(asset_payload)
            self.progressService.add_asset(product['Guid'], response[0]['_responsedata']['assetnum'])
            assetnum = response[0]['_responsedata']['assetnum']

            self.qonicModifications[self.qonicOperation]["FunctionalLocationId"][product['Guid']] = {"PropertySet": "BAC", "Value": functional_location['location']}
            self.qonicModifications[self.qonicOperation]["AssetId"][product['Guid']] = {"PropertySet": "BAC", "Value": assetnum}
            self.synced_locations.add((functional_location['location'], functional_location['lochierarchy'][0]['parent']))
            self.synced_assets.add((product['Guid'], assetnum))
            self.logger.info(f"Synced functional location {functional_location['location']} for product {product['Guid']} in Maximo.")
            self.logger.info(f"Created/updated asset {assetnum} for product {product['Guid']} in Maximo.")
            return response[0]['_responsedata']['assetuid']
        except Exception as e:
            self.logger.error(f"Failed to sync product {product['Guid']}: {e}")
            return None

    def store_progress(self):
        self.progressService.write_final_file()

    def delete_location(self, loc, parent_of):
        try:
            self.maximoClient.delete_location(loc, siteid=self.siteId, orgid=self.orgId)
            self.progressService.delete_location(loc, parent_of.get(loc, ""))
            self.logger.info(f"Deleted location {loc} in Maximo.")
        except MaximoException as e:
            # Fail-safe: delete location if it still has children
            if len(e.args) and "Location has children" in e.args[0]:
                children = self.maximoClient.get_locations_with_parent(loc)
                for child in children:
                    child_location = child.get("location")
                    try:
                        self.delete_location(child_location, parent_of)
                    except Exception as ce:
                        self.logger.error(f"Failed to delete child location {child_location}: {ce}")
            # Fail-safe: delete location if it still has assets
            if len(e.args) and "it is referenced in the ASSET table" in e.args[0]:
                for asset in self.maximoClient.get_assets_with_location(loc, siteid=self.siteId, orgid=self.orgId):
                    assetnum = asset.get("assetnum")
                    try:
                        self.maximoClient.delete_asset(assetnum, siteid=self.siteId, orgid=self.orgId)
                        self.logger.info(f"Deleted asset {assetnum} referencing location {loc} in Maximo.")
                        self.progressService.delete_asset(asset.get("bim_ifcguid"), assetnum)
                    except Exception as ae:
                        self.logger.error(f"Failed to delete asset {assetnum}: {ae}")
                self.delete_location(loc, parent_of)


    def cleanup(self):
        last_run_synced_assets, last_run_synced_locations = self.progressService.load_progress() # Only in case of crash

        with open("synced_data.json") as f:
            output_data = json.load(f)
            synced_assets = output_data.get("synced_assets", [])
            synced_locations = output_data.get("synced_locations", [])

        synced_assets = set(last_run_synced_assets) | set(tuple(item) for item in synced_assets)
        synced_locations = set(last_run_synced_locations) | set(tuple(item) for item in synced_locations)

        children_of = defaultdict(set)
        parent_of = {}
        nodes = set()

        for child, parent in synced_locations:
            nodes.add(child)
            nodes.add(parent)
            parent_of[child] = parent
            children_of[parent].add(child)
            children_of.setdefault(child, set())  # ensure key exists even if leaf

        children_set = set(parent_of.keys())
        parent_candidates = set(children_of.keys())
        roots = [self.parentId] if self.parentId in parent_candidates or self.parentId in nodes else sorted(
            parent_candidates - children_set)

        visited, order = set(), []

        def dfs(n: str):
            if n in visited:
                return
            visited.add(n)
            for c in sorted(children_of.get(n, [])):  # sort for deterministic order
                dfs(c)
            order.append(n)

        for r in roots:
            dfs(r)

        deletion_order = [n for n in order if n in nodes and n != self.parentId]

        deleted_assets = []
        for (ifcguid, assetnum) in synced_assets:
            try:
                self.maximoClient.delete_asset(assetnum, siteid=self.siteId, orgid=self.orgId)
                self.progressService.delete_asset(ifcguid, assetnum)
                self.logger.info(f"Deleted asset {assetnum} in Maximo.")
                deleted_assets.append((ifcguid, assetnum))
            except Exception as e:
                self.logger.error(f"Failed to delete asset {assetnum}: {e}")

        for loc in deletion_order:
            self.delete_location(loc, parent_of)

    def push_modifications_to_qonic(self):
        response = self.qonicClient.modify_model_data(self.projectId, self.modelId, modifications=self.qonicModifications)
        if 'errors' in response and response['errors']:
            for error in response['errors']:
                self.logger.error(f"Failed to push modification: {error}")
        else:
            self.logger.info(
                f"Successfully pushed {len(self.qonicModifications[self.qonicOperation]['FunctionalLocationId'])} functional locations and {len(self.qonicModifications[self.qonicOperation]['AssetId'])} assets to Qonic.")
        return response

