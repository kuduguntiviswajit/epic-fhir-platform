import logging
from pathlib import Path
from config import LOG_PATH
from fhir_platform.auth import FHIRClient
from fhir_platform.extractors.patients import PatientExtractor
from fhir_platform.extractors.observations import ObservationExtractor
from fhir_platform.extractors.medications import MedicationExtractor
from fhir_platform.extractors.encounters import EncounterExtractor
from fhir_platform.transformers.fhir_normalizer import FHIRNormalizer
from fhir_platform.loaders.warehouse import FHIRWarehouse

Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_pipeline(max_records: int = 100):
    """
    Runs the full ETL pipeline for all four FHIR resources.
    Extracts from HAPI FHIR R4, transforms, and loads into SQLite warehouse.
    """
    logger.info("Pipeline started")

    client = FHIRClient()
    normalizer = FHIRNormalizer()
    warehouse = FHIRWarehouse()

    resources = [
        {
            "name": "patients",
            "extractor": PatientExtractor(client),
            "normalizer": normalizer.normalize_patients,
            "table": "patients",
            "id_field": "patient_id"
        },
        {
            "name": "observations",
            "extractor": ObservationExtractor(client),
            "normalizer": normalizer.normalize_observations,
            "table": "observations",
            "id_field": "observation_id"
        },
        {
            "name": "medications",
            "extractor": MedicationExtractor(client),
            "normalizer": normalizer.normalize_medications,
            "table": "medications",
            "id_field": "medication_id"
        },
        {
            "name": "encounters",
            "extractor": EncounterExtractor(client),
            "normalizer": normalizer.normalize_encounters,
            "table": "encounters",
            "id_field": "encounter_id"
        },
    ]

    for resource in resources:
        try:
            logger.info(f"Processing {resource['name']}")
            raw = resource["extractor"].extract(max_records=max_records)
            cleaned = resource["normalizer"](raw)
            loaded = warehouse.load(resource["table"], cleaned, resource["id_field"])
            warehouse.log_pipeline_run(
                resource=resource["name"],
                extracted=len(cleaned),
                loaded=loaded,
                status="success"
            )
            logger.info(f"{resource['name']}: {len(cleaned)} extracted, {loaded} loaded")

        except Exception as e:
            logger.error(f"Pipeline failed for {resource['name']}: {e}")
            warehouse.log_pipeline_run(
                resource=resource["name"],
                extracted=0,
                loaded=0,
                status=f"failed: {str(e)}"
            )

    logger.info("Pipeline completed")
    _print_summary(warehouse)


def _print_summary(warehouse: FHIRWarehouse):
    print("\n--- Pipeline Summary ---")
    for table in ["patients", "observations", "medications", "encounters"]:
        count = warehouse.get_record_count(table)
        print(f"{table.capitalize()}: {count} records in warehouse")
    print("------------------------\n")


if __name__ == "__main__":
    run_pipeline(max_records=100)
