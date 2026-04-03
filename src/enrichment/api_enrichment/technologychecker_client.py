import requests
from typing import Optional, Dict

from config.logger_config import logger


class TechnologyCheckerClient:
    """
    Client for Technologychecker.io: Company data by domain API
    """

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
            logger.warning("No domain provided to TechnologyCheckerClient.")
            return None

        # Real API call
        try:
            logger.info(f"Calling Thechnologychecker.io: company data by domain API: {domain}")

            response = self.session.get(
                f"{self.base_url}/company/{domain}",
                headers={
                    "Authorization": f"Bearer {self.api_key}"
                },
                timeout=10
            )

            # Check HTTP status
            if response.status_code != 200:
                raise Exception(f"Thechnologychecker.io API failed to fetch data "
                                f"for {domain}: {response.status_code} {response.text}")

            self.request_count += 1

            # parse response
            data = response.json().get("data", {})

            if not data:
                logger.warning(f"No data returned from TechnologyCheckerClient for domain: {domain}")
                return None

            return {
                "domain": data.get("domain"),
                "company_name": data.get("company_name"),
                "description": data.get("description"),
                "industry": data.get("industry"),
                "employee_range": data.get("employees"),
                "employee_count": None,
                "country": data.get("country"),
                "city": data.get("city"),
                "founded": data.get("founded"),
                "linkedin_url": data.get("linkedin_url"),
                "crunchbase_url": None,
                "naics_code": None,
                "sic_code": None,
                "annual_revenue": None,
                "revenue_range": None,
                "phone_numbers": None,
                "email_addresses": None,
                "tags": None,
                "technologies": None,
                "type": None,
                "global_ranking": None,
                "source": "technologycheker"
            }

        except Exception as e:
            logger.error(f"Thechnologycheker.io API failed for domain: {domain}: {e}",  exc_info=True)
            return None
