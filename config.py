import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

# Paths
DATABASE_PATH = BASE_DIR / "data" / "fhir_warehouse.db"
LOG_PATH = BASE_DIR / "logs" / "pipeline.log"
REPORTS_PATH = BASE_DIR / "reports"

# FHIR Server
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"

# FHIR Resources to extract
FHIR_RESOURCES = ["Patient", "Observation", "MedicationRequest", "Encounter"]

# Pipeline settings
BATCH_SIZE = 50
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
