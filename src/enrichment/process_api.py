import pandas as pd
from typing import Sequence

from src.enrichment.run_mode import should_process_row, normalize_data, call_api_with_retry, handle_run_mode
from src.utils.validators import validate_api_data
from config.logger_config import logger
from config.variables import RUN_MODE


def process_api_batch(
    df: pd.DataFrame,
    client,
    source_name: str,
    required_fields: Sequence[str],
    call_limit: int,
    seen_domains: set
) -> tuple[list[dict], set, int]:
    """
    Process a batch of company records and enrich them using a specified API client.

    The function prevents duplicate processing by tracking previously seen domains
    and respects API call limits based on the current run mode.

    Args:
        df (pd.DataFrame): Input DataFrame containing company records.
        client: API client instance.
        source_name (str): Identifier for the data source.
        required_fields (Sequence[str]): List or sequence of required fields expected in the API response.
        call_limit (int): Maximum number of API calls allowed for this batch (used in limited/full run modes).
        seen_domains (set): Set of domains that have already been processed.

    Returns:
        tuple:
            - list[dict]: List of enriched rows as dictionaries
            - set: Updated set of seen domains
            - int: Number of successful API calls made

       """

    enriched_rows: list[dict] = []
    calls_made = 0

    logger.info(f"RUN MODE: {RUN_MODE}")

    for _, row in df.iterrows():

        # Before API call controls how many rows get processed
        if calls_made >= call_limit:
            logger.info(f"Call limit reached: {call_limit}")
            break

        domain = row.get("domain")

        if not should_process_row(domain, seen_domains):
            continue

        # ---------------- RUN MODE ----------------
        mode_action = handle_run_mode(domain, calls_made, call_limit)

        if mode_action == "continue":
            seen_domains.add(domain)
            continue

        if mode_action == "mock":
            enriched = row.to_dict()
            enriched.update({
                "company_name": f"mock_{source_name}",
                "industry": "mock_industry",
                "employee_range": "mock_size",
                "country": "mock_location",
                "source": f"{source_name}_mock",
            })
            enriched_rows.append(enriched)
            seen_domains.add(domain)
            continue

        if mode_action == "break":
            break

        # ---------------- REAL API ----------------
        api_data = call_api_with_retry(client, domain, source_name)

        if not api_data:
            continue

        if not validate_api_data(api_data, required_fields):
            logger.warning(f"Incomplete data for {domain}")
            continue

        enriched = normalize_data(row, api_data, source_name)

        enriched_rows.append(enriched)
        seen_domains.add(domain)
        calls_made += 1

    return enriched_rows, seen_domains, calls_made
