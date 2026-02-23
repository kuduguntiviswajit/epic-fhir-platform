import logging
from fhir_platform.auth import FHIRClient

logger = logging.getLogger(__name__)


class ObservationExtractor:
    """
    Extracts Observation resources (lab results, vitals) from the
    FHIR R4 server and normalizes them into flat dictionaries.
    """

    def __init__(self, client: FHIRClient):
        self.client = client

    def extract(self, max_records: int = 100) -> list[dict]:
        logger.info(f"Extracting up to {max_records} Observation records")
        resources = self.client.get_all_pages("Observation", max_records=max_records)
        return [self._normalize(r) for r in resources]

    def _normalize(self, resource: dict) -> dict:
        value = resource.get("valueQuantity", {})
        code = resource.get("code", {})
        coding = code.get("coding", [{}])[0]
        subject = resource.get("subject", {}).get("reference", "")
        patient_id = subject.replace("Patient/", "") if subject else None

        reference_range = resource.get("referenceRange", [{}])[0]
        low = reference_range.get("low", {}).get("value")
        high = reference_range.get("high", {}).get("value")

        numeric_value = value.get("value")
        is_abnormal = False
        if numeric_value is not None:
            if low is not None and numeric_value < low:
                is_abnormal = True
            if high is not None and numeric_value > high:
                is_abnormal = True

        return {
            "observation_id": resource.get("id"),
            "patient_id": patient_id,
            "status": resource.get("status"),
            "category": resource.get("category", [{}])[0]
                .get("coding", [{}])[0].get("code"),
            "code": coding.get("code"),
            "display": coding.get("display") or code.get("text"),
            "value": numeric_value,
            "unit": value.get("unit"),
            "reference_low": low,
            "reference_high": high,
            "is_abnormal": is_abnormal,
            "effective_date": resource.get("effectiveDateTime"),
        }

