import logging
from fhir_platform.auth import FHIRClient

logger = logging.getLogger(__name__)


class PatientExtractor:
    """
    Extracts Patient resources from the FHIR R4 server and
    normalizes them into flat dictionaries for warehouse loading.
    """

    def __init__(self, client: FHIRClient):
        self.client = client

    def extract(self, max_records: int = 100) -> list[dict]:
        logger.info(f"Extracting up to {max_records} Patient records")
        resources = self.client.get_all_pages("Patient", max_records=max_records)
        return [self._normalize(r) for r in resources]

    def _normalize(self, resource: dict) -> dict:
        name = resource.get("name", [{}])[0]
        full_name = " ".join(
            name.get("given", []) + [name.get("family", "")]
        ).strip()

        address = resource.get("address", [{}])[0]

        return {
            "patient_id": resource.get("id"),
            "full_name": full_name or "Unknown",
            "gender": resource.get("gender", "unknown"),
            "birth_date": resource.get("birthDate"),
            "city": address.get("city"),
            "state": address.get("state"),
            "country": address.get("country"),
            "marital_status": resource.get("maritalStatus", {}).get("text"),
            "language": resource.get("communication", [{}])[0]
                .get("language", {}).get("text"),
        }

