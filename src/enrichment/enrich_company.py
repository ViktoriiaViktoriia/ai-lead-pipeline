from pathlib import Path
import pandas as pd

from config.logger_config import logger
from config.config import (PRIMARY_API_KEY_ABSTRACT, BASE_URL_ABSTRACT,
                           BASE_URL_TECHNOLOGYCHEKER, API_TOKEN_TECHNOLOGYCHEKER)
from config.variables import (RUN_MODE, TEST_API_CALLS_LIMIT, FULL_API_CALLS_LIMIT,
                              TOP_LEADS_LIMIT, CHECKPOINT_INTERVAL)
from src.enrichment.selection.lead_prioritizer import select_top_leads
from src.enrichment.api_enrichment.abstract_client import AbstractClient
from src.enrichment.api_enrichment.technologychecker_client import TechnologyCheckerClient
from src.enrichment.process_api import process_api_batch


def enrich_company_chunk(
    chunk: pd.DataFrame,
    seen_domains: set,
    abstract_client: AbstractClient,
    tech_client: TechnologyCheckerClient
) -> tuple[pd.DataFrame, set]:
    """
    Enrich companies in a single chunk using Abstract API for top 100 leads and
     Technologychecker API for next  100 leads.

    Args:
        chunk (pd.DataFrame):
            Input DataFrame containing company records to enrich. Must include a "domain" column.

        seen_domains (set):
            Set of domains already processed in previous batches or earlier in the pipeline.
            This set is updated in-place to prevent duplicate API calls.

        abstract_client (AbstractClient):
            Client instance for interacting with the Abstract API.
            Must provide `abstract_required_fields` for response validation.

        tech_client (TechnologyCheckerClient):
            Client instance for interacting with the TechnologyChecker API.
            Must provide `tech_required_fields` for response validation.

    Returns:
        tuple:
            - pd.DataFrame:
                DataFrame containing enriched company data from both APIs.
                If no data is enriched, returns an empty DataFrame with expected columns.

    """

    df_top = select_top_leads(chunk, limit=TOP_LEADS_LIMIT)
    df_rest = chunk.drop(df_top.index)
    #df_rest = chunk.loc[~chunk.index.isin(df_top.index)]

    logger.info(f"Top leads (Abstract): {len(df_top)}")
    logger.info(f"Next leads (TechnologyChecker): {len(df_rest)}")

    call_limit = TEST_API_CALLS_LIMIT if RUN_MODE == "limited" else FULL_API_CALLS_LIMIT

    # ---------------- Abstract ----------------
    abstract_rows, seen_domains, abstract_calls = process_api_batch(
        df=df_top,
        client=abstract_client,
        source_name="abstract",
        required_fields=abstract_client.abstract_required_fields,
        call_limit=call_limit,
        seen_domains=seen_domains
    )

    # ---------------- TechnologyChecker ----------------
    tech_rows, seen_domains, tech_calls = process_api_batch(
        df=df_rest,
        client=tech_client,
        source_name="technologychecker",
        required_fields=tech_client.tech_required_fields,
        call_limit=call_limit,
        seen_domains=seen_domains
    )

    logger.info(f"Abstract calls made: {abstract_calls}")
    logger.info(f"TechChecker calls made: {tech_calls}")

    all_rows = abstract_rows + tech_rows

    df_enriched = (
        pd.DataFrame(all_rows)
        if all_rows
        else pd.DataFrame(columns=chunk.columns.tolist() + ["source"])
    )

    return df_enriched, seen_domains


def enrich_company_parquet(
    input_path: Path,
    output_path: Path,
    seen_domains_file: Path
):
    """
    Process parquet files for company enrichment and persist results.

    This function reads parquet files from the input directory, enriches
    company data in chunks, and saves the aggregated results. It maintains
    a cache of processed domains to avoid duplicate API calls and supports
    checkpointing for long-running jobs.

    Args:
        input_path (Path): Directory containing input parquet files with company data.
        output_path (Path): Directory where the final enriched parquet file will be saved.
        seen_domains_file (Path): Path to a CSV file used to persist processed domains between runs.
                                  If the file exists, it will be loaded at the start of the pipeline.

    Returns:
        None
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    seen_domains = set()

    # Load cache
    if seen_domains_file.exists():
        df_seen = pd.read_csv(seen_domains_file)
        if "domain" in df_seen.columns:
            seen_domains.update(df_seen["domain"].dropna().tolist())
            logger.info(f"Loaded {len(seen_domains)} seen domains from cache.")
        else:
            logger.warning("Seen domains file missing 'domain' column.")

    output_path.mkdir(parents=True, exist_ok=True)

    abstract_client = AbstractClient(api_key=PRIMARY_API_KEY_ABSTRACT, base_url=BASE_URL_ABSTRACT)
    tech_client = TechnologyCheckerClient(api_key=API_TOKEN_TECHNOLOGYCHEKER, base_url=BASE_URL_TECHNOLOGYCHEKER)

    parquet_files = sorted(input_path.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files to process.")

    all_enriched = []
    total_enriched = 0

    for chunk_number, file in enumerate(parquet_files):
        logger.info(f"[{chunk_number+1}/{len(parquet_files)}] Processing file: {file}")

        try:
            df = pd.read_parquet(file, engine="pyarrow")

            df_enriched, seen_domains = enrich_company_chunk(
                df,
                seen_domains,
                abstract_client,
                tech_client
            )

            if not df_enriched.empty:
                all_enriched.append(df_enriched)
                total_enriched += len(df_enriched)

                logger.info(f"Chunk {chunk_number}: enriched {len(df_enriched)} companies")

            if (chunk_number + 1) % CHECKPOINT_INTERVAL == 0:
                pd.DataFrame({"domain": list(seen_domains)}).to_csv(seen_domains_file, index=False)
                logger.info("Checkpoint: saved seen domains")

        except Exception as e:
            logger.error(f"Failed processing {file}: {e}")
            continue

    if all_enriched:
        final_df = pd.concat(all_enriched, ignore_index=True)
        final_output = output_path / "companies_enriched_final.parquet"
        final_df.to_parquet(final_output, index=False)

        logger.info(f"Saved final enriched dataset: {final_output}")
        logger.info(f"Total enriched companies: {total_enriched}")

    pd.DataFrame({"domain": list(seen_domains)}).to_csv(seen_domains_file, index=False)
    logger.info(f"Final seen domains saved: {seen_domains_file}")

    logger.info("Enrichment pipeline completed successfully.")
