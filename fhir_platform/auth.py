import requests
import logging
from config import FHIR_BASE_URL, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)


class FHIRClient:
    """
    HTTP client for the HAPI FHIR R4 public server.
    Handles requests, pagination, and error handling.
    """

    def __init__(self):
        self.base_url = FHIR_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/fhir+json",
            "Content-Type": "application/fhir+json"
        })

    def get(self, resource: str, params: dict = None) -> dict:
        """
        Fetches a FHIR resource bundle from the server.
        """
        url = f"{self.base_url}/{resource}"
        try:
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching {resource}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error fetching {resource}: {e}")
            raise

    def get_all_pages(self, resource: str, params: dict = None, max_records: int = 100) -> list:
        """
        Fetches all pages of a FHIR resource up to max_records.
        Handles FHIR pagination via the next link in the bundle.
        """
        records = []
        params = params or {}
        params["_count"] = self.batch_size if hasattr(self, "batch_size") else 20

        url = f"{self.base_url}/{resource}"

        while url and len(records) < max_records:
            try:
                response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                bundle = response.json()

                entries = bundle.get("entry", [])
                records.extend([e["resource"] for e in entries if "resource" in e])

                url = None
                params = {}
                for link in bundle.get("link", []):
                    if link.get("relation") == "next":
                        url = link.get("url")
                        break

            except Exception as e:
                logger.error(f"Error fetching page for {resource}: {e}")
                break

        logger.info(f"Fetched {len(records)} records for {resource}")
        return records[:max_records]

