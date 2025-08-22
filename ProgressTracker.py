import json
import os
from typing import Tuple, Set, Optional


class ProgressTracker:
    def __init__(self, progress_path="progress.jsonl", final_path="synced_data.json"):
        self.progress_path = progress_path
        self.final_path = final_path
        self.synced_assets: Set[Tuple[str, str]] = set()
        self.synced_locations: Set[Tuple[str, str]] = set()

        if os.path.exists(self.progress_path):
            self.load_progress()

    def load_progress(self):
        """Replay all progress records to rebuild current state."""
        if not os.path.exists(self.progress_path):
            return self.synced_assets, self.synced_locations

        with open(self.progress_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    self._apply_record(record)
                except Exception:
                    continue  # Skip malformed lines

        return self.synced_assets, self.synced_locations

    def _apply_record(self, record: dict):
        """Apply a single record to in-memory sets."""
        if record["type"] == "create_location":
            self.synced_locations.add((record["location"], record["parent"]))
        elif record["type"] == "delete_location":
            self.synced_locations.discard((record["location"], record["parent"]))
        elif record["type"] == "create_asset":
            self.synced_assets.add((record["ifcguid"], record["assetnum"]))
        elif record["type"] == "delete_asset":
            self.synced_assets.discard((record["ifcguid"], record["assetnum"]))

    def _write_record(self, record: dict):
        with open(self.progress_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def add_location(self, location: str, parent: str):
        self.synced_locations.add((location, parent))
        self._write_record({
            "type": "create_location",
            "location": location,
            "parent": parent
        })

    def delete_location(self, location: str, parent: str):
        self.synced_locations.discard((location, parent))
        self._write_record({
            "type": "delete_location",
            "location": location,
            "parent": parent
        })

    def add_asset(self, ifcguid: str, assetnum: str):
        self.synced_assets.add((ifcguid, assetnum))
        self._write_record({
            "type": "create_asset",
            "ifcguid": ifcguid,
            "assetnum": assetnum
        })

    def delete_asset(self, ifcguid: str, assetnum: str):
        self.synced_assets.discard((ifcguid, assetnum))
        self._write_record({
            "type": "delete_asset",
            "ifcguid": ifcguid,
            "assetnum": assetnum
        })

    def write_final_file(self):
        """Apply progress to final file by replaying recorded events and saving result."""

        # Start with existing final state
        if os.path.exists(self.final_path):
            with open(self.final_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            existing_assets = set(tuple(a) for a in existing.get("synced_assets", []))
            existing_locations = set(tuple(l) for l in existing.get("synced_locations", []))
        else:
            existing_assets = set()
            existing_locations = set()

        # Reset in-memory sets, replay progress file to apply operations
        self.synced_assets.clear()
        self.synced_locations.clear()
        self.load_progress()

        # Apply merged result
        merged_assets = sorted((existing_assets | self.synced_assets) - (existing_assets - self.synced_assets))
        merged_locations = sorted(
            (existing_locations | self.synced_locations) - (existing_locations - self.synced_locations))

        output_data = {
            "synced_assets": merged_assets,
            "synced_locations": [list(loc) for loc in merged_locations],
        }

        with open(self.final_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)

        if os.path.exists(self.progress_path):
            os.remove(self.progress_path)

    def reset_progress(self):
        """Clear progress file and in-memory state."""
        self.synced_assets.clear()
        self.synced_locations.clear()
        if os.path.exists(self.progress_path):
            os.remove(self.progress_path)

    def get_synced_assets(self) -> Set[Tuple[str, str]]:
        return self.synced_assets

    def get_synced_locations(self) -> Set[Tuple[str, str]]:
        return self.synced_locations


_progress_tracker = ProgressTracker()

def get_progress_tracker() -> ProgressTracker:
    return _progress_tracker