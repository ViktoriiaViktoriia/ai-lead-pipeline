import requests
from typing import Optional, Dict

from config.logger_config import logger


class AbstractClient:
    """
    Client for Abstract Company Enrichment API
    """

    def __init__(self, api_key: str, base_url: str, mock: bool = False):
        self.api_key = api_key
        self.base_url = base_url
        self.mock = mock
        self.session = requests.Session()
        self.request_count = 0

    def enrich_company(self, domain: str) -> Optional[Dict]:
        """
        Enrich company by domain
        """

        if not domain:
            logger.warning("Empty domain provided")
            return None

        # Mock mode
        if self.mock:
            logger.info(f"[MOCK] Enriching {domain}")
            return {
                "industry_api": "Software",
                "company_size_api": 200,
                "country_api": "FI",
                "founded_api": 2015,
                "linkedin_url_api": None,
                "naics_code_api": None,
                "sic_code_api": None,
                "annual_revenue_api": None,
                "data_source": "mock"
            }

        # Real API call
        try:
            logger.info(f"Calling Abstract API: {domain}")

            response = self.session.get(
                self.base_url,
                params={
                    "api_key": self.api_key,
                    "domain": domain
                },
                timeout=10
            )

            # Check HTTP status
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data for {domain}: {response.status_code} {response.text}")

            self.request_count += 1

            # parse response
            data = response.json()

            return {
                "industry_api": data.get("industry"),
                "company_size_api": data.get("employee_count"),
                "country_api": data.get("country"),
                "founded_api": data.get("year_founded"),
                "linkedin_url_api": data.get("linkedin_url"),
                "naics_code_api": data.get("naics_code"),
                "sic_code_api": data.get("sic_code"),
                "annual_revenue_api": data.get("annual_revenue"),
                "data_source": "abstract"
            }

        except Exception as e:
            logger.error(f"Abstract API failed for {domain}: {e}",  exc_info=True)
            return None
