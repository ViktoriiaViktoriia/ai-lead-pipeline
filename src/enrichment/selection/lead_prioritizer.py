import pandas as pd

from config.variables import (NORDICS, EU, SIZE_SCORE_MAP, PRIORITY_WEIGHTS)
from config.logger_config import logger


def assign_geo_priority(country: str) -> int:
    """
    Assign geographic priority score.
    """
    try:
        if pd.isna(country):
            return 1

        if country in NORDICS:
            return 3
        elif country in EU:
            return 2

        return 1

    except Exception as e:
        logger.error(f"Error assigning geo priority for country={country}: {e}")
        return 1


def compute_priority_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute priority score for enrichment selection.
    """
    try:
        logger.info("Starting priority score computation")

        df = df.copy()

        # Geo priority
        df["geo_priority"] = df["country"].apply(assign_geo_priority)

        # Size score
        df["size_score"] = (
            df["size_category"]
            .map(SIZE_SCORE_MAP)
            .fillna(1)
        )

        # Missing data score
        df["data_missing_score"] = (
            df[["industry", "size_category", "country"]]
            .isna()
            .sum(axis=1)
        )

        # Final score
        df["priority_score"] = (
            df["geo_priority"] * PRIORITY_WEIGHTS["geo"] +
            df["size_score"] * PRIORITY_WEIGHTS["size"] +
            df["data_missing_score"] * PRIORITY_WEIGHTS["missing"]
        )

        logger.info("Priority score computation completed")

        return df

    except Exception as e:
        logger.error(f"Error computing priority score: {e}", exc_info=True)
        raise


def select_top_leads(df: pd.DataFrame, limit: int = 100) -> pd.DataFrame:
    """
    Select top leads for API enrichment based on priority score.
    """
    try:
        logger.info("Selecting top leads for enrichment")

        initial_count = len(df)

        # Base filtering
        df_filtered = df[
            (df["company_name"].notna()) &
            (df["domain"].notna()) &
            (df["is_valid_domain"] == True)
        ].copy()

        logger.info(
            f"Filtered valid leads: {len(df_filtered)} out of {initial_count}"
        )

        # Compute scores
        df_scored = compute_priority_score(df_filtered)

        # Sort
        df_sorted = df_scored.sort_values(
            by="priority_score",
            ascending=False
        )

        # Select top N
        df_top = df_sorted.head(limit)

        logger.info(
            f"Selected top {len(df_top)} leads for enrichment"
        )

        logger.info(
            f"Top priority sample:\n"
            f"{df_top[['company_name', 'priority_score']].head().to_dict(orient='records')}"
        )

        return df_top

    except Exception as e:
        logger.error(f"Error selecting top leads: {e}", exc_info=True)
        raise
