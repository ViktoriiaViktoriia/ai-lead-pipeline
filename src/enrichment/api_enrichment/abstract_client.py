import requests
from typing import Optional, Dict

from config.logger_config import logger


class AbstractClient:
    """
    Client for Abstract Company Enrichment API
    """

    REQUIRED_FIELDS = ["company_name", "industry", "country"]

    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.request_count = 0

    def enrich_company(self, domain: str) -> Optional[Dict]:
        """
        Enrich company by domain
        """

        if not domain:
            logger.warning("No domain provided to Abstract company enrichment API.")
            return None

        # Real API call
        try:
            logger.info(f"Calling Abstract company enrichment API: {domain}")

            response = self.session.get(
                self.base_url,
                params={
                    "api_key": self.api_key,
                    "domain": domain
                },
                timeout=10
            )

            # Check HTTP status
            response.raise_for_status()

            self.request_count += 1

            # parse response
            data = response.json()

            if not data:
                logger.warning(f"No data returned from Abstract company enrichment API for domain: {domain}")
                return None

            return {
                "domain": data.get("domain"),
                "company_name": data.get("company_name"),
                "description": data.get("description"),
                "industry": data.get("industry"),
                "employee_range": data.get("employee_range"),
                "employee_count": data.get("employee_count"),
                "country": data.get("country"),
                "city": data.get("city"),
                "founded": data.get("year_founded"),
                "linkedin_url": data.get("linkedin_url"),
                "crunchbase_url": data.get("crunchbase_url"),
                "naics_code": data.get("naics_code"),
                "sic_code": data.get("sic_code"),
                "annual_revenue": data.get("annual_revenue"),
                "revenue_range": data.get("revenue_range"),
                "phone_numbers": data.get("phone_numbers", []),
                "email_addresses": data.get("email_addresses", []),
                "tags": data.get("tags", []),
                "technologies": data.get("technologies", []),
                "type": data.get("type"),
                "global_ranking": data.get("global_ranking"),
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Abstract API failed for domain: {domain}: {e}",  exc_info=True)
            return None
