from pathlib import Path
import pandas as pd
from typing import Optional
from datetime import datetime

from config.logger_config import logger
from config.config import API_ENRICHED_DATA_PATH, DATA_DIR
from config.variables import (TEST_API_CALLS_LIMIT, FULL_API_CALLS_LIMIT,
                              TOP_LEADS_LIMIT, EU)

from src.enrichment.selection.lead_prioritizer import select_top_leads
from src.enrichment.api_enrichment.abstract_client import AbstractClient
from src.enrichment.api_enrichment.technologychecker_client import TechnologyCheckerClient
from src.enrichment.api_enrichment.clients import create_abstract_client, create_tech_client
from src.enrichment.process_api import process_api_batch


def enrich_company_chunk(
    df_abstract: pd.DataFrame,
    df_tech: pd.DataFrame,
    seen_domains: set[str],
    mode: str,
    abstract_client: AbstractClient,
    tech_client: TechnologyCheckerClient,
    call_limit: int,
):
    """
    Execute Abstract API calls and Technologychecker API calls.

    Args:
        df_abstract (pd.DataFrame): Input DataFrame containing company records to enrich using Abstract API.
        df_tech (pd.DataFrame): Input DataFrame containing company records to enrich using Technologychecker API.
        seen_domains (set): Set of domains already processed in previous batches or earlier in the pipeline.
                            This set is updated in-place to prevent duplicate API calls.
        mode (str): Run mode identifier.
        abstract_client (AbstractClient): Client instance for interacting with the Abstract API.
                                          Must provide `abstract_required_fields` for response validation.
        tech_client (TechnologyCheckerClient): Client instance for interacting with the TechnologyChecker API.
                                               Must provide `tech_required_fields` for response validation.
        call_limit (int): API calls limit.

    Returns:
        tuple:
            - pd.DataFrame: DataFrame containing enriched company data from both APIs.
                If no data is enriched, returns an empty DataFrame with expected columns.

    """
    logger.info("Starting API enrichment step")

    abstract_calls = 0
    tech_calls = 0

    # ---------------- Abstract -------------------------
    abstract_rows, seen_domains, abstract_calls = process_api_batch(
        df=df_abstract,
        client=abstract_client,
        source_name="abstract",
        required_fields=abstract_client.abstract_required_fields,
        call_limit=call_limit,
        seen_domains=seen_domains,
        mode=mode,
        calls_made=abstract_calls,
    )

    # ---------------- TechnologyChecker ----------------
    tech_rows, seen_domains, tech_calls = process_api_batch(
        df=df_tech,
        client=tech_client,
        source_name="technologychecker",
        required_fields=tech_client.tech_required_fields,
        call_limit=call_limit,
        seen_domains=seen_domains,
        mode=mode,
        calls_made=tech_calls,
    )

    logger.info(f"Abstract calls made: {abstract_calls}/{call_limit}")
    logger.info(f"TechChecker calls made: {tech_calls}/{call_limit}")

    enriched_rows = abstract_rows + tech_rows

    return enriched_rows, seen_domains


def enrich_company_parquet(
    input_path: Path,
    output_path: Path,
    seen_domains_file: Path,
    mode: str,
    abstract_client: Optional[AbstractClient] = None,
    tech_client: Optional[TechnologyCheckerClient] = None
):
    """
    Process parquet files for company enrichment and persist results.

    This function reads parquet files from the input directory, enriches
    company data, and saves the aggregated results. It maintains
    a cache of processed domains to avoid duplicate API calls and supports
    checkpointing for long-running jobs.

    Args:
        input_path (Path): Directory containing input parquet files with company data.
        output_path (Path): Directory where the final enriched parquet file will be saved.
        seen_domains_file (Path): Path to a CSV file used to persist processed domains between runs.
                                  If the file exists, it will be loaded at the start of the pipeline.
        mode (str): Run mode identifier.
        abstract_client (AbstractClient): Client instance for interacting with the Abstract API.
        tech_client (TechnologyCheckerClient): Client instance for interacting with the TechnologyChecker API.

    Returns:
        None
    """
    # Check path exists
    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    # Load seen domains
    seen_domains = set()
    if not seen_domains_file.exists():
        logger.info("Seen domains file not found. Creating new one.")
        seen_domains_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"domain": []}).to_csv(seen_domains_file, index=False)
    else:
        try:
            df_seen = pd.read_csv(seen_domains_file)

            if "domain" not in df_seen.columns:
                logger.warning("Seen domains file missing 'domain' column.")
            else:
                seen_domains = set(df_seen["domain"].dropna().tolist())
                logger.info(f"Loaded {len(seen_domains)} seen domains from cache.")
        except (OSError, IOError) as e:
            logger.warning(f"Failed to load seen domains: {e}")
        except pd.errors.EmptyDataError:
            logger.warning("Seen domains file is empty. Reinitializing.")
            seen_domains = set()

    # Load data
    parquet_files = sorted(input_path.glob("*.parquet"))
    logger.info(f"Found {len(parquet_files)} parquet files to process.")

    if not parquet_files:
        logger.warning("No parquet files found to process.")
        return

    dfs = []

    try:
        for file in parquet_files:
            logger.info(f"Loading file: {file}")

            df = pd.read_parquet(file)

            df["country"] = df["country"].astype(str).str.strip().str.upper()
            logger.info(f"Unique country values: {df['country'].unique()[:10]}")
            logger.info(f"Head (5) country values: {df['country'].value_counts().head(5)}")

            # Filter by country (region)
            df = df[df["country"].isin(EU)]
            logger.info(f"Unique country values: {df['country'].unique()[:5]}")

            dfs.append(df)

        full_df = pd.concat(dfs, ignore_index=True)

        logger.info(f"Total rows after EU filter: {len(full_df)}")

        raw_snapshot_path = DATA_DIR / "full_companies_eu_raw/raw_leads_eu_snapshot.csv"

        # Save full df (EU region) snapshot
        if not raw_snapshot_path.exists():
            logger.info("Dataset (full_companies_eu_raw) is not found. Creating new one.")
            # full_df.to_csv(DATA_DIR / "full_companies_eu_raw/raw_leads_eu_snapshot.csv")
            full_df.to_csv(raw_snapshot_path)
        else:
            logger.info("Dataset raw_leads_eu_snapshot.csv already exists.")

        # Global scoring of potential leads (select top leads for API enrichment)
        df_top = select_top_leads(full_df, limit=TOP_LEADS_LIMIT)

        top_eu_snapshot_path = API_ENRICHED_DATA_PATH / "companies_eu_top/top100_leads_eu_snapshot.csv"

        # Save df (top 100 companies EU)
        if not top_eu_snapshot_path.exists():
            logger.info("Dataset (companies_eu_top) is not found. Creating new one.")
            df_top.to_csv(top_eu_snapshot_path)
        else:
            logger.info("Dataset (companies_eu_top) already exists.")

        df_rest = full_df.drop(df_top.index)

        rest_eu_snapshot_path = API_ENRICHED_DATA_PATH / "companies_eu_rest_raw/rest_leads_eu_raw_snapshot.csv"

        # Save df (rest of companies EU region)
        if not rest_eu_snapshot_path.exists():
            logger.info("Dataset (companies_eu_rest_raw) is not found. Creating new one.")
            df_rest.to_csv(rest_eu_snapshot_path)
        else:
            logger.info("Dataset (companies_eu_rest_raw) already exists.")

        # Remove already processed domains
        df_top = df_top[~df_top["domain"].isin(seen_domains)]
        logger.info(f"Totally final candidates after filtering: {len(df_top)}")

        if mode in ("dry", "mock"):
            mock_df = full_df.head(10)
            logger.info(f"MODE {mode}: limiting to 5 rows: {len(mock_df)}.")

        # Split df_top 50/50
        df_abstract = df_top.iloc[:50]
        df_tech = df_top.iloc[50:100]

        # API calls limits per client
        call_limit = TEST_API_CALLS_LIMIT if mode == "limited" else FULL_API_CALLS_LIMIT

        if abstract_client is None:
            abstract_client = create_abstract_client()

        if tech_client is None:
            tech_client = create_tech_client()

        # Execution
        enriched_rows, seen_domains = enrich_company_chunk(
            df_abstract=df_abstract,
            df_tech=df_tech,
            seen_domains=seen_domains,
            mode=mode,
            abstract_client=abstract_client,
            tech_client=tech_client,
            call_limit=call_limit,
        )

        # Save enriched data
        if mode not in ("dry", "mock") and enriched_rows:
            output_path.mkdir(parents=True, exist_ok=True)

            output_file = output_path / f"enriched_{datetime.now().strftime('%Y_%m_%d_%H%M')}.csv"

            pd.DataFrame(enriched_rows).to_csv(
                output_file,
                index=False
            )
            logger.info(f"Saved enriched data: {output_file}")
        else:
            logger.info(f"{mode.upper()} mode: skipping enriched data save.")

        # Save seen domains
        if mode not in ("dry", "mock"):
            try:
                pd.DataFrame({"domain": list(seen_domains)}).to_csv(
                    seen_domains_file,
                    index=False
                )
                logger.info(f"Saved seen domains: {len(seen_domains)}")
            except Exception as e:
                logger.error(f"Failed saving seen domains: {e}", exc_info=True)
        else:
            logger.info(f"{mode.upper()} mode: skipping seen_domains save")

        logger.info("Pipeline completed successfully")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Cntrl + C)")

        if mode not in ("dry", "mock"):
            try:
                pd.DataFrame({"domain": list(seen_domains)}).to_csv(
                    seen_domains_file,
                    index=False
                )
                logger.info("Saved seen domains after interruption")
            except (OSError, IOError) as e:
                logger.error(f"Failed saving after interrupt: {e}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise

    logger.info("Enrichment pipeline completed successfully.")
