import logging
from fhir_platform.auth import FHIRClient

logger = logging.getLogger(__name__)


class EncounterExtractor:
    """
    Extracts Encounter resources from the FHIR R4 server and
    normalizes them into flat dictionaries.
    """

    def __init__(self, client: FHIRClient):
        self.client = client

    def extract(self, max_records: int = 100) -> list[dict]:
        logger.info(f"Extracting up to {max_records} Encounter records")
        resources = self.client.get_all_pages("Encounter", max_records=max_records)
        return [self._normalize(r) for r in resources]

    def _normalize(self, resource: dict) -> dict:
        subject = resource.get("subject", {}).get("reference", "")
        patient_id = subject.replace("Patient/", "") if subject else None

        period = resource.get("period", {})
        start = period.get("start")
        end = period.get("end")

        coding = resource.get("class", {})
        type_coding = resource.get("type", [{}])[0].get("coding", [{}])[0]

        participant = resource.get("participant", [{}])[0]
        provider = participant.get("individual", {}).get("display")

        return {
            "encounter_id": resource.get("id"),
            "patient_id": patient_id,
            "status": resource.get("status"),
            "class": coding.get("code"),
            "type": type_coding.get("display"),
            "start": start,
            "end": end,
            "provider": provider,
            "reason": resource.get("reasonCode", [{}])[0]
                .get("coding", [{}])[0].get("display"),
            "location": resource.get("location", [{}])[0]
                .get("location", {}).get("display"),
        }

