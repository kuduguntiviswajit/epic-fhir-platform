import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class FHIRNormalizer:
    """
    Cleans and normalizes raw FHIR records before warehouse loading.
    Handles encoding issues, date standardization, and invalid references.
    """

    def normalize_patients(self, records: list[dict]) -> list[dict]:
        cleaned = []
        for r in records:
            r["full_name"] = self._fix_encoding(r.get("full_name", ""))
            r["birth_date"] = self._normalize_date(r.get("birth_date"))
            r["gender"] = r.get("gender", "unknown").lower().strip()
            r["patient_id"] = str(r["patient_id"]) if r.get("patient_id") else None
            cleaned.append(r)
        return cleaned

    def normalize_observations(self, records: list[dict]) -> list[dict]:
        cleaned = []
        for r in records:
            r["patient_id"] = self._clean_reference(r.get("patient_id"))
            r["effective_date"] = self._normalize_date(r.get("effective_date"))
            r["value"] = self._safe_float(r.get("value"))
            r["display"] = self._fix_encoding(r.get("display", ""))
            cleaned.append(r)
        return cleaned

    def normalize_medications(self, records: list[dict]) -> list[dict]:
        cleaned = []
        for r in records:
            r["patient_id"] = self._clean_reference(r.get("patient_id"))
            r["authored_on"] = self._normalize_date(r.get("authored_on"))
            r["medication_name"] = self._fix_encoding(r.get("medication_name", ""))
            r["status"] = r.get("status", "unknown").lower().strip()
            cleaned.append(r)
        return cleaned

    def normalize_encounters(self, records: list[dict]) -> list[dict]:
        cleaned = []
        for r in records:
            r["patient_id"] = self._clean_reference(r.get("patient_id"))
            r["start"] = self._normalize_date(r.get("start"))
            r["end"] = self._normalize_date(r.get("end"))
            r["status"] = r.get("status", "unknown") or "unknown"
            r["class"] = r.get("class", "unknown") or "unknown"
            cleaned.append(r)
        return cleaned

    def _fix_encoding(self, text: str) -> str:
        if not text:
            return text
        try:
            return text.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

    def _normalize_date(self, date_str: str) -> str:
        if not date_str:
            return None
        try:
            date_str = re.sub(r"\.\d+", "", date_str)
            for fmt in (
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(
                        date_str.replace("Z", "+00:00"), fmt
                    ).strftime("%Y-%m-%d")
                except ValueError:
                    continue
        except Exception:
            pass
        return date_str[:10] if len(date_str) >= 10 else date_str

    def _clean_reference(self, reference: str) -> str:
        if not reference:
            return None
        if reference.startswith("http"):
            return None
        return reference.replace("Patient/", "").strip()

    def _safe_float(self, value) -> float:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None