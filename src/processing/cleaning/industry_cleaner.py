import pandas as pd
from typing import Optional

from config.logger_config import logger
from config.variables import INDUSTRY_MAPPING


def normalize_industry(industry: str) -> Optional[str]:
    """
    Normalize industry values using mapping.
    """

    if pd.isna(industry):
        return None

    industry = industry.strip().lower()

    return INDUSTRY_MAPPING.get(industry, industry)  # fallback


def clean_industry(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply industry normalization.
    """
    logger.info("Standardizing industry")

    df["industry_raw"] = df["industry"]
    df["industry"] = df["industry"].apply(normalize_industry)

    return df
