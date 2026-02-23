import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

# Paths
DATABASE_PATH = BASE_DIR / "data" / "fhir_warehouse.db"
LOG_PATH = BASE_DIR / "logs" / "pipeline.log"
REPORTS_PATH = BASE_DIR / "reports"

# Epic FHIR Sandbox
EPIC_BASE_URL = "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4"
EPIC_TOKEN_URL = "https://fhir.epic.com/interconnect-fhir-oauth/oauth2/token"
EPIC_CLIENT_ID = os.getenv("EPIC_CLIENT_ID")
EPIC_PRIVATE_KEY_PATH = BASE_DIR / "epic_fhir" / "private_key.pem"

# FHIR Resources to extract
FHIR_RESOURCES = ["Patient", "Observation", "MedicationRequest", "Encounter"]

# Pipeline settings
BATCH_SIZE = 50
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
