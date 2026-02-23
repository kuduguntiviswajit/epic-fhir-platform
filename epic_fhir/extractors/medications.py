import logging
from epic_fhir.auth import FHIRClient

logger = logging.getLogger(__name__)


class MedicationExtractor:
    """
    Extracts MedicationRequest resources from the FHIR R4 server
    and normalizes them into flat dictionaries.
    """

    def __init__(self, client: FHIRClient):
        self.client = client

    def extract(self, max_records: int = 100) -> list[dict]:
        logger.info(f"Extracting up to {max_records} MedicationRequest records")
        resources = self.client.get_all_pages("MedicationRequest", max_records=max_records)
        return [self._normalize(r) for r in resources]

    def _normalize(self, resource: dict) -> dict:
        subject = resource.get("subject", {}).get("reference", "")
        patient_id = subject.replace("Patient/", "") if subject else None

        medication = resource.get("medicationCodeableConcept", {})
        coding = medication.get("coding", [{}])[0]

        dosage = resource.get("dosageInstruction", [{}])[0]
        dose = dosage.get("doseAndRate", [{}])[0].get("doseQuantity", {})

        return {
            "medication_id": resource.get("id"),
            "patient_id": patient_id,
            "status": resource.get("status"),
            "intent": resource.get("intent"),
            "medication_code": coding.get("code"),
            "medication_name": coding.get("display") or medication.get("text"),
            "authored_on": resource.get("authoredOn"),
            "dose_value": dose.get("value"),
            "dose_unit": dose.get("unit"),
            "route": dosage.get("route", {}).get("text"),
            "frequency": dosage.get("text"),
        }