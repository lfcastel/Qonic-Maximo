import json
from collections import defaultdict

from dotenv import load_dotenv; load_dotenv()
from LoggingSetup import setup_logging, get_logger
setup_logging(log_dir="logs", base_name="qonic_maximo_cleanup.log")
from MaximoClient import MaximoClient

orgId = "BRU-ORG"
siteId = "BRU"
parentId = "BUILDINGS"
maximoClient = MaximoClient()
logger = get_logger()

synced_assets = set()
synced_locations = set()

with open("synced_data.json") as f:
    output_data = json.load(f)
    synced_assets = output_data.get("synced_assets", [])
    synced_locations = output_data.get("synced_locations", [])

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
roots = [parentId] if parentId in parent_candidates or parentId in nodes else sorted(parent_candidates - children_set)

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

deletion_order = [n for n in order if n in nodes and n != parentId]


deleted_assets = []
for (ifcguid, assetnum) in synced_assets:
    try:
        maximoClient.delete_asset(assetnum, siteid=siteId, orgid=orgId)
        logger.info(f"Deleted asset {assetnum} in Maximo.")
        deleted_assets.append((ifcguid, assetnum))
    except Exception as e:
        logger.error(f"Failed to delete asset {assetnum}: {e}")


deleted_locations = []
for loc in deletion_order:
    try:
        maximoClient.delete_location(loc, siteid=siteId, orgid=orgId)
        logger.info(f"Deleted location {loc} in Maximo.")
        deleted_locations.append((loc, parent_of.get(loc, "")))
    except Exception as e:
        logger.error(f"Failed to delete location {loc}: {e}")

remaining_assets = [a for a in synced_assets if tuple(a) not in deleted_assets]
remaining_locations = [l for l in synced_locations if tuple(l) not in deleted_locations]

with open("synced_data.json", "w") as f:
    json.dump({
        "synced_assets": remaining_assets,
        "synced_locations": remaining_locations
    }, f, indent=4)


