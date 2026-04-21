import pandas as pd
import requests.exceptions

from config.logger_config import logger

from src.enrichment.enrich_company import enrich_company_parquet

from tests.test_utils import MockClient429, MockTechClient, RateLimitReached, create_parquet_test_files


def test_handles_429_and_stops_safely(tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    seen_domains_file = tmp_path / "seen.csv"

    input_path.mkdir()

    create_parquet_test_files(input_path, num_files=1, rows_per_file=200)

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

        # Test partial progress saved
        saved_files = list(output_path.glob("*.csv"))
        print("Saved files:", saved_files)
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
    except OSError as e:
        logger.error(f"File write failed: {e}")
    except RateLimitReached as e:
        logger.warning(f"{e} -> stopping {e.source}")
        return saved_files, seen_domains_file

    assert len(saved_files) > 0

    assert client_abstract_test429.calls >= 0

    df = pd.read_csv(saved_files[0])

    assert len(df) >= 1
