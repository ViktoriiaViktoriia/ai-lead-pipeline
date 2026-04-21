from pathlib import Path
import argparse

from src.ingestion.load_company_leads import load_raw_leads
from src.processing.cleaning.clean_data import clean_data
from src.enrichment.enrich_company import enrich_company_parquet
from src.enrichment.api_enrichment.clients import create_abstract_client, create_tech_client

from config.config import (RAW_DATA_PATH, CHUNKED_DATA_PATH, CLEANED_DATA_PATH,
                           API_ENRICHED_DATA_PATH, SEEN_DOMAINS_PATH)
from config.logger_config import logger


def parse_args():
    parser = argparse.ArgumentParser(description="Company leads pipeline runner")

    parser.add_argument("--ingestion", action="store_true", help="Run ingestion stage")
    parser.add_argument("--cleaning", action="store_true", help="Run cleaning stage")
    parser.add_argument("--enrichment", action="store_true", help="Run enrichment stage")

    parser.add_argument(
        "--mode",
        type=str,
        default="dry",
        choices=["dry", "mock", "limited", "full"],
        help="Run mode"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    logger.info(
        f"Pipeline args: ingestion={args.ingestion}, cleaning={args.cleaning}, "
        f"enrichment={args.enrichment}"
    )

    # Dependency information
    if args.cleaning and not args.ingestion:
        logger.warning(
            "Cleaning requested without ingestion. "
            "Assuming chunked data already exists."
        )

    # Ingestion stage
    if args.ingestion:
        logger.info("====== INGESTION STAGE STARTED ======")
        try:
            load_raw_leads(RAW_DATA_PATH, CHUNKED_DATA_PATH)
            logger.info("====== INGESTION STAGE COMPLETED ======")
        except Exception as e:
            logger.error(f"Ingestion stage failed: {e}", exc_info=True)
            raise  # stop pipeline
    else:
        logger.info("Skipping ingestion stage")

    # Cleaning stage
    if args.cleaning:
        logger.info("====== CLEANING STAGE STARTED ======")

        # Safety check before running cleaning
        if not Path(CHUNKED_DATA_PATH).exists():
            logger.error(
                "Chunked data not found. Cannot run cleaning. "
                "Run ingestion first."
            )
            raise FileNotFoundError("Missing input data for cleaning stage")

        try:
            clean_data(CHUNKED_DATA_PATH, CLEANED_DATA_PATH)
            logger.info("====== CLEANING STAGE COMPLETED ======")
        except Exception as e:
            logger.error(f"Cleaning stage failed: {e}", exc_info=True)
            raise  # stop pipeline
    else:
        logger.info("Skipping cleaning stage")

    # Enrichment stage
    if args.enrichment:
        logger.info("====== ENRICHMENT STAGE STARTED ======")

        # Safety check before running enrichment
        if not Path(CLEANED_DATA_PATH).exists():
            logger.error("Cleaned data not found. Cannot run enrichment.")
            raise FileNotFoundError("Missing input data for enrichment stage")

        try:
            logger.info(f"Run mode: {args.mode}")

            # Override RUN_MODE dynamically
            mode = args.mode

            if mode == "full":
                confirm = input("! FULL RUN will consume API credits. Type 'YES' to continue: ")
                if confirm.strip() != "YES":
                    logger.info("Full run cancelled by user")
                    return

            if mode in ["limited", "full"]:
                abstract_client = create_abstract_client()
                tech_client = create_tech_client()
            else:
                abstract_client = None
                tech_client = None

            enrich_company_parquet(
                CLEANED_DATA_PATH,
                API_ENRICHED_DATA_PATH,
                SEEN_DOMAINS_PATH,
                mode,
                abstract_client,
                tech_client
            )
            logger.info("====== ENRICHMENT STAGE COMPLETED ======")
        except Exception as e:
            logger.error(f"Enrichment stage failed: {e}", exc_info=True)
            raise  # stop pipeline
    else:
        logger.info("Skipping enrichment stage")


if __name__ == "__main__":
    main()
