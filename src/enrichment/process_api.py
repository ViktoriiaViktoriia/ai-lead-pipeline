import pandas as pd
from typing import Sequence
from datetime import datetime

from config.logger_config import logger
from config.config import (API_ENRICHED_DATA_PATH, SEEN_DOMAINS_PATH)

from src.enrichment.run_mode import should_process_row, normalize_data, call_api_with_retry, handle_run_mode
from src.utils.validators import validate_api_data


def process_api_batch(
    df: pd.DataFrame,
    client,
    source_name: str,
    required_fields: Sequence[str],
    call_limit: int,
    seen_domains: set,
    mode: str,
    calls_made: int = 0,
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
        mode (str): Identifier for the run mode.
        calls_made (int): Estimate made API calls.

    Returns:
        tuple:
            - list[dict]: List of enriched rows as dictionaries
            - set: Updated set of seen domains
            - int: Number of successful API calls made

       """

    enriched_rows: list[dict] = []

    logger.info(f"RUN MODE: {mode}")

    dry_counter = 0

    for _, row in df.iterrows():

        # Before API call controls how many rows get processed
        if calls_made >= call_limit:
            logger.info(f"Call limit reached: {call_limit}")
            break

        domain = row.get("domain")

        if not domain:
            continue

        if not should_process_row(domain, seen_domains):
            continue

        # ---------------- RUN MODE ----------------
        mode_action = handle_run_mode(domain, calls_made, call_limit, mode)

        if mode == "dry" and calls_made >= 3:
            break
        dry_counter += 1

        if mode_action == "continue":
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

        calls_made += 1

        # Save enriched row instantly
        enriched_rows.append(enriched)

        # Save seen domains in a CSV
        if mode in ("dry", "mock"):
            logger.info(f"{mode.upper()} mode: skipping enriched rows save")
            logger.info(f"{mode.upper()} mode: skipping seen_domains persistence")
        else:
            try:
                output_path = API_ENRICHED_DATA_PATH
                output_path.mkdir(parents=True, exist_ok=True)

                # Timestamp per run (created once per process ideally)
                timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")

                output_file = output_path / f"{source_name}_{timestamp}.csv"

                pd.DataFrame([enriched]).to_csv(
                    output_file,
                    mode="a",
                    header=not output_file.exists(),
                    index=False
                )

                logger.info(f"Total enriched companies: {calls_made}")

            except Exception as e:
                logger.error(f"Failed to save enriched row for {domain}: {e}", exc_info=True)

            # Seen domains (saves only real runs)
            try:
                if domain not in seen_domains:
                    seen_domains.add(domain)

                    seen_domains_file = SEEN_DOMAINS_PATH
                    seen_domains_file.parent.mkdir(parents=True, exist_ok=True)

                    pd.DataFrame({"domain": [domain]}).to_csv(
                       seen_domains_file,
                       mode="a",
                       header=not seen_domains_file.exists(),
                       index=False
                    )
                    logger.info(f"{source_name}: {domain} saved as seen domains into: {seen_domains_file}")
            except Exception as e:
                logger.error(f"Failed to save seen domain {domain}: {e}", exc_info=True)

    return enriched_rows, seen_domains, calls_made
