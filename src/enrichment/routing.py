from enum import Enum
import pandas as pd

from config.logger_config import logger


class Route(Enum):
    AI = "ai"
    RULE = "rule"
    SKIP = "skip"


def assign_routes(df: pd.DataFrame, top_n: int = 200) -> pd.DataFrame:
    """
    Assign routing decisions based on TOP-N scoring strategy.

    Top N rows → AI enrichment
    Remaining rows → rule-based enrichment
    Invalid rows → skip
    """

    required_columns = {"score", "domain", "company_name"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Sort by score descending
    df_sorted = df.sort_values("score", ascending=False).copy()

    # Initialize route column
    df_sorted["route"] = Route.RULE.value

    # Identify invalid rows
    invalid_mask = (
        df_sorted["domain"].astype(str).str.strip().eq("") &
        df_sorted["company_name"].astype(str).str.strip().eq("")
    )

    df_sorted.loc[invalid_mask, "route"] = Route.SKIP.value

    # Assign AI to top N valid rows
    valid_df = df_sorted[~invalid_mask]
    top_n = min(top_n, len(valid_df))

    ai_indices = valid_df.head(top_n).index
    df_sorted.loc[ai_indices, "route"] = Route.AI.value

    logger.info(
        "Routing completed",
        extra={
            "Assigned rows to AI enrichment": len(ai_indices),
            "Total rows": len(df_sorted),
            "Skipped rows": int(invalid_mask.sum())
        }
    )

    return df_sorted
