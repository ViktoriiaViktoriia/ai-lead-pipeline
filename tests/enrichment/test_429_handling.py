import pandas as pd
import requests.exceptions
from unittest.mock import patch

from config.logger_config import logger

from src.enrichment.enrich_company import enrich_company_parquet

from tests.test_utils import MockClient429, MockTechClient, RateLimitReached, create_parquet_test_files, create_test_df


@patch("pandas.DataFrame.to_csv")
@patch("src.enrichment.enrich_company.select_top_leads")
def test_handles_429_and_stops_safely(mock_select_top_leads, mock_to_csv, tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    seen_domains_file = tmp_path / "seen.csv"

    input_path.mkdir()

    create_parquet_test_files(input_path, num_files=2, rows_per_file=100)

    # Force non-empty enrichment input
    mock_select_top_leads.return_value = create_test_df(100)

    parquet_files = list(input_path.glob("*.parquet"))
    assert len(parquet_files) > 0, "No parquet files were generated"

    for f in parquet_files:
        df = pd.read_parquet(f)
        logger.info(f"Rows after EU filter: {len(df)}")
        assert not df.empty, f"Empty dataframe in {f}"

    client_abstract_test429 = MockClient429(fail_after=3)
    tech_client_test = MockTechClient()

    saved_files = []

    try:
        enrich_company_parquet(
            input_path=input_path,
            output_path=output_path,
            seen_domains_file=seen_domains_file,
            mode="limited",
            abstract_client=client_abstract_test429,
            tech_client=tech_client_test,
        )

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
    except OSError as e:
        logger.error(f"File write failed: {e}")
    except RateLimitReached as e:
        logger.warning(f"{e} -> stopping {e.source}")
        return saved_files, seen_domains_file

    # API was actually used
    assert client_abstract_test429.calls >= 1

    # Tech client was triggered (if expected in flow)
    assert tech_client_test.calls >= 1

    # Pipeline attempted to save output
    assert mock_to_csv.called

    # Pipeline attempted to save output
    assert mock_to_csv.call_count >= 1

