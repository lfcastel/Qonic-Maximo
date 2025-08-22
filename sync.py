import os
from dotenv import load_dotenv; load_dotenv()
from LoggingSetup import setup_logging
setup_logging(log_dir="logs", base_name="qonic_maximo_sync.log")

from QonicMaximoSync import QonicMaximoSync

# productFilters = {"Guid": "1wddXsDr17pA_ixjMwTDUA"}
# productFilters = { "Guid": "0ghk9ho2X6Pe20aU4np$1K" }
# productFilters = { "Guid": "3YxdsPhef6Tw3MjY2qVfXL" }
# productFilters = { "Guid": "1wddXsDr17pA_ixjMwTDH0" }
codeFilter = ['AHU', 'CHHEPU', 'COVAHV','FACOUN', 'FAN', 'FIDASMDA', 'FIFISYHV','FIFISYPL', 'FLMEDE', 'PUHV']
productFilters = {}

if __name__ == "__main__":
    projectId = os.environ["QONIC_PROJECT_ID"]
    modelId = os.environ["QONIC_MODEL_ID"]
    qonicMaximoSync = QonicMaximoSync(projectId, modelId)
    qonicMaximoSync.init_qonic_data(productFilters, codeFilter)
    qonicMaximoSync.sync_locations()
    qonicMaximoSync.sync_products()
    qonicMaximoSync.store_progress()
    qonicMaximoSync.push_modifications_to_qonic()