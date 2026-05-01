import pandas as pd
import os

from config.logger_config import logger

from src.enrichment.selection.preprocessing.load_validate_select import process_rows


def run_ai_enrichment_pipeline(
    df: pd.DataFrame,
    ai_client,
    output_path: str,
    mode: str = "rule",
    batch_size: int = 100
) -> None:

    batch = []
    total_rows = len(df)

    logger.info(f"Starting enrichment pipeline | mode={mode} | rows={total_rows}")

    try:
        df["priority_score_norm"] = df["priority_score"] / df["priority_score"].max()

        enriched_rows = process_rows(df, ai_client)

        for row in enriched_rows:
            if row.get("priority_score_norm", 0) > 0.8:
                _save_partial([row], output_path)
                continue

            if row:
                batch.append(row)

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

        logger.info("Pipeline finished")


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

    except Exception as e:
        logger.exception(f"Failed to save batch: {e}")
