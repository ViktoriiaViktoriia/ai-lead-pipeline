from pathlib import Path
import argparse

from src.ingestion.load_company_leads import load_raw_leads
from src.processing.cleaning.clean_data import clean_data

from config.config import RAW_DATA_PATH, CHUNKED_DATA_PATH, CLEANED_DATA_PATH
from config.logger_config import logger


def parse_args():
    parser = argparse.ArgumentParser(description="Company leads pipeline runner")

    parser.add_argument("--ingestion", action="store_true", help="Run ingestion stage")
    parser.add_argument("--cleaning", action="store_true", help="Run cleaning stage")

    return parser.parse_args()


def main():
    args = parse_args()

    logger.info(f"Pipeline args: ingestion={args.ingestion}, cleaning={args.cleaning}")

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


if __name__ == "__main__":
    main()
