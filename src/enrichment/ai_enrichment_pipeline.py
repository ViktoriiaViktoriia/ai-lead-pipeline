import pandas as pd
import os

from config.logger_config import logger
from config.variables import MODE_CONFIG

from src.enrichment.selection.preprocessing.load_validate_select import process_rows
from src.enrichment.routing import Route, assign_routes


def run_ai_enrichment_pipeline(
    df: pd.DataFrame,
    ai_client,
    output_path: str,
    mode: str = "rule"
) -> int:
    """
    Run the AI enrichment pipeline on a dataset of company records.

    Pipeline workflow:
        1. Normalize priority scores
        2. Load mode-specific configuration
        3. Assign enrichment routes (AI, rule-based, skip)
        4. Apply row-level enrichment
        5. Save enriched results in batches or immediately for high-priority rows

    Args:
        df (pd.DataFrame): Input dataset containing company records.
        ai_client: AI client implementation used for AI enrichment.
        output_path (str): Path where enriched results will be saved.
        mode (str, optional): Pipeline execution mode. Determines routing and batching configuration.
                              Defaults to "rule".

    Returns:
        int: Total number of processed rows successfully saved.
    """
    batch = []
    total_rows = len(df)
    processed_count = 0

    logger.info(f"Starting enrichment pipeline | mode={mode} | rows={total_rows}")

    try:
        max_score = df["priority_score"].max()

        if max_score > 0:
            df["priority_score_norm"] = df["priority_score"] / max_score
        else:
            df["priority_score_norm"] = pd.Series(0.0, index=df.index)

        config = MODE_CONFIG[mode]
        top_n = config["top_n"]
        batch_size = config["batch_size"]

        df = assign_routes(df, top_n=top_n)

        logger.info(
            "Route distribution",
            extra={
                "ai_rows": len(df[df["route"] == Route.AI.value]),
                "rule_rows": len(df[df["route"] == Route.RULE.value]),
                "skip_rows": len(df[df["route"] == Route.SKIP.value]),
            }
        )

        enriched_rows = process_rows(df, ai_client)

        for row in enriched_rows:
            if row.get("priority_score_norm", 0) > 0.8:
                _save_partial([row], output_path)
                processed_count += 1
                continue

            if row:
                batch.append(row)
                processed_count += 1

            if len(batch) >= batch_size:
                _save_partial(batch, output_path)
                batch.clear()

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user. Saving progress...")

    except Exception as e:
        logger.exception(f"Row enrichment failed: {e}")

    finally:
        if batch:
            _save_partial(batch, output_path)

        logger.info(f"Pipeline finished. Total processed rows: {processed_count}")

    return processed_count


def _save_partial(batch: list, output_path: str):
    df_batch = pd.DataFrame(batch)

    try:
        df_batch.to_csv(
            output_path,
            mode="a",
            header=not os.path.exists(output_path),
            index=False
        )
        logger.info(f"Saved batch of {len(df_batch)} rows")

    except OSError as e:
        logger.exception(f"Filesystem error: {e}")
    except Exception as e:
        logger.exception(f"Failed to save batch: {e}")
