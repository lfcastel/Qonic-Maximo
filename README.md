# Qonic ↔ Maximo Integration – BAC Project

This project provides tooling to sync Qonic asset and location data with IBM Maximo, specifically tailored for the
Brussels Airport Company (BAC) environment.

---

## Project Structure

### Core Modules

| File                    | Description                                                                                                                        |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `AssetMapper.py`        | Maps Qonic asset data into Maximo-compliant format. Handles classstructure IDs, asset fields, and payload formatting.              |
| `LocationMapper.py`     | Converts Qonic spatial location objects to Maximo location payloads. Also handles recursive parent syncing.                        |
| `MaximoClient.py`       | Handles communication with the Maximo REST API, including session setup, object structure operations, error handling, and logging. |
| `QonicClient.py`        | Connects to the Qonic backend to fetch assets and spatial locations. Supports filtering and pagination.                            |
| `bsdd/BssdMapping.json` | Mapping definitions for BAC-specific asset classes and fields between Qonic and Maximo.                                            |
| `bsdd/BsddService.py`   | Service to fetch BAC-specific data from the BSSD system, used to map between Qonic and Maximo.                                     |

### Supporting Scripts

| File         | Description                                                                                                     |
|--------------|-----------------------------------------------------------------------------------------------------------------|
| `sync.py`    | Main script to perform the sync operation from Qonic to Maximo. Fetches data, maps it, and pushes it to Maximo. |
| `cleanup.py` | Cleans up previously synced assets and locations in Maximo, based on stored sync history in `synced_data.json`. |

### Utilities

| File               | Description                                                                                      |
|--------------------|--------------------------------------------------------------------------------------------------|
| `LoggingSetup.py`  | Initializes and configures global logging for both console and file output.                      |
| `synced_data.json` | Tracks which assets and locations have already been synced, this is used for cleanup operations. |
| `requirements.txt` | Python dependencies for this project.                                                            |

---

## Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt

2. **Configure environment variables**
   Create a .env file by copying the example:
   ```
   cp .env.example .env
   ```

   Then, fill in the required values in the `.env` file:
    - `MAXIMO_API_KEY`: Your Maximo API key.

3. **Run a sync**

   ```bash
   python sync.py
   ```

4. **(Optional) Clean up synced data**

   ```bash
   python cleanup.py
   ```

# Diving Deeper
## What this script does exactly

This script syncs selected Qonic products with Maximo in three steps:

1. Location Sync
   - For each product, it finds the spatial location and creates the full location hierarchy in Maximo (parents first).
2. Asset Creation
   - Converts the product into a Maximo asset. Links it to the correct functional location in Maximo. This includes all the properties and
     classifications as defined by the class structure defined in Maximo.
 3. Qonic Update
    - Pushes the newly created Maximo AssetId and FunctionalLocationId back to Qonic.

## Maximo Object Structures

We had to create a custom object structure `QONIC_MXAPILOCATIONS` which allows setting parent relationships for
locations. The existing endpoints in Maximo do not support setting parent locations directly.

## BAC-Specific Mappings

The `bsdd/BssdMapping.json` file contains mappings specific to BAC's asset classes and fields. The `BsddService.py`
module fetches data from the BSSD system to assist in these mappings. If the BAC classifications change in BSSD, the
mappings may need to be updated accordingly by running the `bsdd/BsddService.py` script.

