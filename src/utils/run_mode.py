import pandas as pd
import time
from typing import Sequence

from config.logger_config import logger
from config.variables import RUN_MODE, MAX_RETRIES, BACKOFF_SEC
from src.utils.helpers import rate_limited
from src.utils.validators import validate_api_data


def handle_run_mode(domain: str, calls_made: int, limit: int):
    """
    Controls execution based on RUN_MODE.

    Args:
        domain (str): Domain name.
        calls_made (int): Number of API calls made.
        limit (int): API calls limit.

    Returns: ("continue" | "mock" | "break" | "proceed")
    """

    if RUN_MODE == "dry":
        logger.info(f"[DRY RUN] Would call API for: {domain}")
        return "continue"

    if RUN_MODE == "mock":
        logger.info(f"[MOCK_RUN] Mock API call for {domain}")
        return "mock"

    if RUN_MODE == "limited" and calls_made >= limit:
        logger.info(f"API test limit reached.")
        return "break"

    if RUN_MODE == "full" and calls_made >= limit:
        logger.info(f"API full limit reached.")
        return "break"

    return "proceed"


def should_process_row(domain: str, seen_domains: set) -> bool:
    """
    This function filters out rows that:
    - Do not contain a valid domain
    - Have already been processed (present in seen_domains)

    Args:
        domain (str): Domain extracted from the current row.
        seen_domains (set): Set of domains that have already been enriched.

    Returns:
        bool: True if the row should be processed, False otherwise.

    """
    if not domain:
        logger.info("No domain, skipping.")
        return False

    if domain in seen_domains:
        logger.info(f"Domain already enriched: {domain}, skipping.")
        return False

    return True


def call_api_with_retry(client, domain: str, source_name: str) -> dict | None:
    """
    Call an external API to enrich company data with retry logic,
    rate limiting, and exponential backoff.

    Args:
        client: API client instance.
        domain (str): Company domain to enrich.
        source_name (str): Name of the data source (used for logging).

    Returns:
        dict | None:
            - Dictionary containing enriched company data if successful
            - None if all attempts fail or no data is returned
    """
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"[{source_name}] Calling API for {domain}, attempt {attempt+1}")

            with rate_limited():
                api_data = client.enrich_company(domain)

            if not api_data:
                logger.warning(f"No data returned for {domain}")
                return None

            return api_data

        except Exception as e:
            logger.warning(f"{source_name} attempt {attempt+1} failed for {domain}: {e}")
            time.sleep(BACKOFF_SEC * (2 ** attempt))

    logger.error(f"All attempts failed for {source_name}: {domain}")
    return None


def normalize_data(row: pd.Series, api_data: dict, source_name: str) -> dict:
    """
    This function safely updates the original row data with values from the API response.
    It prevents overwriting existing values with None to avoid data loss.

    Args:
        row (pd.Series): Original row from the input DataFrame.
        api_data (dict): Normalized API response data.
        source_name (str): Name of the data source.

    Returns:
        dict: Enriched row as a dictionary with merged data and source label.
    """
    enriched = row.to_dict()

    # safe merge (prevents overwriting with None)
    for key, value in api_data.items():
        if value is not None:
            enriched[key] = value

    enriched["source"] = source_name
    return enriched


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
