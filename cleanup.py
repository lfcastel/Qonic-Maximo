import json
from collections import defaultdict

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

for asset in synced_assets:
    maximoClient.delete_asset(asset, siteid=siteId, orgid=orgId)
    logger.info(f"Deleted asset {asset} in Maximo.")

for loc in deletion_order:
    maximoClient.delete_location(loc, siteid=siteId, orgid=orgId)
    logger.info(f"Deleted location {loc} in Maximo.")

with open("synced_data.json", "w") as f:
    json.dump({
        "synced_assets": [],
        "synced_locations": []
    }, f, indent=4)


