import os
from dotenv import load_dotenv; load_dotenv()
from LoggingSetup import setup_logging
setup_logging(log_dir="logs", base_name="qonic_maximo_sync.log")

from QonicMaximoSync import QonicMaximoSync

if __name__ == "__main__":
    projectId = os.environ["QONIC_PROJECT_ID"]
    modelId = os.environ["QONIC_MODEL_ID"]
    qonicMaximoSync = QonicMaximoSync(projectId, modelId)
    qonicMaximoSync.cleanup()
    qonicMaximoSync.store_progress()
