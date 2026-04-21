import pandas as pd

from src.enrichment.api_enrichment.technologychecker_client import TechnologyCheckerClient
from src.enrichment.api_enrichment.abstract_client import AbstractClient


class MockAbstractClient(AbstractClient):
    def __init__(self, fail_after=None):
        super().__init__(api_key="test", base_url="test")

        self.calls = 0
        self.failed_after = fail_after
        self.abstract_required_fields = ["company_name"]

    def enrich_company(self, domain):
        self.calls += 1
        return {
            "company_name": f"mock_{domain}",
            "industry": "test",
            "country": "SE"
        }


class MockTechClient(TechnologyCheckerClient):
    def __init__(self, fail_after=None):
        super().__init__(api_key="test", base_url="test")

        self.calls = 0
        self.failed_after = fail_after
        self.tech_required_fields = ["company_name"]

    def enrich_company(self, domain: str) -> dict:
        self.calls += 1

        return {
            "company_name": f"mock_{domain}",
            "industry": "test",
            "country": "SE"
        }


class MockClient429(AbstractClient):
    def __init__(self, name="mock_api", fail=False, fail_after=2):
        super().__init__(api_key="test", base_url="test")
        self.name = name
        self.calls = 0
        self.fail = fail
        self.fail_after = fail_after
        self.abstract_required_fields = ["company_name"]

    def enrich_company(self, domain):
        self.calls += 1

        if self.fail:
            raise Exception(f"429 from {self.name}")

        if self.fail_after and self.calls > self.fail_after:
            raise Exception(f"429 limit reached for {self.name}")

        return {
            "company_name": f"mock_{domain}",
            "industry": "test",
            "country": "SE",
            "source": self.name,
            "status": "ok",
        }


class RateLimitReached(Exception):
    def __init__(self, source: str, message: str = "Rate limit reached"):
        self.source = source
        super().__init__(f"{message}: {source}")


# Create fake dataset
def create_test_df(n=200):
    return pd.DataFrame({
        "domain": [f"test{i}.com" for i in range(n)],
        "country": ["FI"] * n,
        "industry": ["tech"] * n,
        "company_name": [f"company{i}" for i in range(n)],
        "size_category": "1K - 2K",
        "is_valid_domain": True,
    })


# Save as multiple parquet files
def create_parquet_test_files(tmp_path, num_files=2, rows_per_file=100):
    for i in range(num_files):
        df = create_test_df(rows_per_file)
        df.to_parquet(tmp_path / f"file_{i}.parquet")


