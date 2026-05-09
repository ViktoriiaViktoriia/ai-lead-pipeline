from pathlib import Path
import pandas as pd
import argparse

from config.config import (RAW_DATA_PATH, CHUNKED_DATA_PATH, CLEANED_DATA_PATH,
                           API_ENRICHED_DATA_PATH, SEEN_DOMAINS_PATH, AI_ENRICHED_DATA_PATH,
                           RULE_ENRICHED_DATA_PATH)
from config.logger_config import logger

from src.ingestion.load_company_leads import load_raw_leads
from src.processing.cleaning.clean_data import clean_data
from src.enrichment.selection.preprocessing.load_validate_select import (load_data, select_candidates)
from src.enrichment.enrich_company import enrich_company_parquet
from src.enrichment.api_enrichment.clients import create_abstract_client, create_tech_client
from src.enrichment.ai_enrichment_pipeline import run_ai_enrichment_pipeline
from src.enrichment.ai_enrichment.ai_clients import create_mock_ai_client, create_ai_client


def parse_args():
    parser = argparse.ArgumentParser(description="Company leads pipeline runner")

    parser.add_argument("--ingestion", action="store_true", help="Run ingestion stage")
    parser.add_argument("--cleaning", action="store_true", help="Run cleaning stage")
    parser.add_argument("--enrichment", action="store_true", help="Run enrichment stage")

    parser.add_argument(
        "--mode",
        type=str,
        default="dry",
        choices=["dry", "mock", "rule", "limited", "full"],
        help="Run mode"
    )

    parser.add_argument(
        "--client",
        type=str,
        default="mock",
        choices=["mock", "abstract", "tech", "ai"],
        help="Client type"
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
            logger.error("Cleaned data not found. Can not run enrichment.")
            raise FileNotFoundError("Missing input data for enrichment stage")

        try:
            logger.info(f"Run mode: {args.mode}")
            logger.info(f"Client type: {args.client}")

            # Override RUN_MODE dynamically
            mode = args.mode

            if mode == "full":
                confirm = input("! FULL RUN will consume API credits. Type 'YES' to continue: ")
                if confirm.strip() != "YES":
                    logger.info("Full run cancelled by user")
                    return

            client = args.client

            # API enrichment
            if mode in ["limited", "full"] and client in ["abstract", "tech"]:
                abstract_client = create_abstract_client()
                tech_client = create_tech_client()
            else:
                abstract_client = None
                tech_client = None

            if abstract_client or tech_client:
                enrich_company_parquet(
                    CLEANED_DATA_PATH,
                    API_ENRICHED_DATA_PATH,
                    SEEN_DOMAINS_PATH,
                    mode,
                    abstract_client,
                    tech_client
                )

            # AI enrichment
            # load data for AI/rule-based enrichment
            input_data_paths = [
                API_ENRICHED_DATA_PATH/"companies_eu_rest_raw",
                API_ENRICHED_DATA_PATH / "api_enriched"   # use previously generated API enrichment output as input
            ]

            all_dfs = []

            for input_path in input_data_paths:
                if not input_path.exists():
                    logger.error("API enriched data not found. Can not continue enrichment.")
                    raise FileNotFoundError("Missing input data for AI enrichment stage")

                for file in input_path.glob("*.csv"):
                    df = load_data(file)
                    all_dfs.append(df)

            if not all_dfs:
                raise ValueError("No input CSV files found for enrichment")

            final_df = pd.concat(all_dfs, ignore_index=True)

            # select data
            df_selected = select_candidates(final_df)
            logger.info(f"Selected rows: {len(df_selected)}"
                        f"Total rows: {len(final_df)}"
                        )

            # output paths
            ai_enriched_output_path = AI_ENRICHED_DATA_PATH
            rule_enriched_output_path = RULE_ENRICHED_DATA_PATH

            if mode in ["limited", "full"] and client == "ai":
                ai_client = create_ai_client()
                output_path = ai_enriched_output_path
                log_info = "====== AI ENRICHMENT COMPLETED ======"

            elif mode == "rule" and client == "mock":
                ai_client = create_mock_ai_client()
                output_path = rule_enriched_output_path
                log_info = "====== RULE-BASED ENRICHMENT COMPLETED ======"

            else:
                raise ValueError(
                    f"Unsupported mode/client combination: mode={mode}, client={client}"
                )

            process_rows_rule_ai_enrich = run_ai_enrichment_pipeline(
                df=df_selected,
                ai_client=ai_client,
                output_path=output_path,
                mode=args.mode
            )

            if process_rows_rule_ai_enrich > 0:
                logger.info(f"{log_info}")

            logger.info("====== ENRICHMENT STAGE COMPLETED ======")

        except ValueError as e:
            logger.exception(f"Configuration error: {e}")
        except FileNotFoundError as e:
            logger.exception(f"Input file not found: {e}")
        except Exception as e:
            logger.error(f"Enrichment stage failed: {e}", exc_info=True)
            raise  # stop pipeline

    else:
        logger.info("Skipping enrichment stage")


if __name__ == "__main__":
    main()
