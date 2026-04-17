import pandas as pd

from src.enrichment.enrich_company import enrich_company_parquet
from src.enrichment.api_enrichment.abstract_client import AbstractClient
from src.enrichment.api_enrichment.technologychecker_client import TechnologyCheckerClient


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

        # if self.fail_after and self.calls > self.fail_after:
        #    raise Exception("429 Too many requests")
        return {
            "company_name": f"mock_{domain}",
            "industry": "test",
            "country": "SE"
        }


# Create fake dataset
def create_test_df(n=100):
    return pd.DataFrame({
        "domain": [f"test{i}.com" for i in range(n)],
        "country": ["SE"] * n,
        "industry": ["tech"] * n,
        "company_name": [f"company{i}" for i in range(n)],
        "size_category": "1K - 2K",
        "is_valid_domain": True,
    })


# Save as multiple parquet files
def create_parquet_files(tmp_path, num_files=2, rows_per_file=50):
    for i in range(num_files):
        df = create_test_df(rows_per_file)
        df.to_parquet(tmp_path / f"file_{i}.parquet")


# Test api limit (mock clients)
def test_global_api_limit(tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    seen_domains_file = tmp_path / "seen.csv"

    input_path.mkdir()

    # Create multiple parquet files
    create_parquet_files(input_path, num_files=2, rows_per_file=50)

    # Mock clients
    abstract_client_test = MockAbstractClient()
    tech_client_test = MockTechClient()

    # Set strict limit
    test_limit_mock = 2

    # Run pipeline
    enrich_company_parquet(
        input_path=input_path,
        output_path=output_path,
        seen_domains_file=seen_domains_file,
        mode="limited",
        abstract_client=abstract_client_test,
        tech_client=tech_client_test,
    )

    assert abstract_client_test.calls > 0
    assert tech_client_test.calls > 0

    assert abstract_client_test.calls <= test_limit_mock
    assert tech_client_test.calls <= test_limit_mock

    output_files = list(output_path.glob("enriched_*.csv"))

    assert len(output_files) == 1

    df = pd.read_csv(output_files[0])

    assert not df.empty
    assert "source" in df.columns
    assert set(df["source"]) <= {"abstract", "technologychecker"}





