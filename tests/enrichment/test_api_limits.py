import pandas as pd

from src.enrichment.enrich_company import enrich_company_parquet

from tests.test_utils import MockAbstractClient, MockTechClient, create_parquet_files


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
