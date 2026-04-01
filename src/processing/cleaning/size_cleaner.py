import pandas as pd
from typing import Optional

from config.logger_config import logger
from config.variables import SIZE_MAPPING


def normalize_size(size: str) -> Optional[str]:
    """
    Normalize company size into standard buckets.
    """

    if pd.isna(size):
        return None

    size = size.strip().lower()

    return SIZE_MAPPING.get(size, size)  # fallback to original


def clean_size(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply size normalization.
    """
    logger.info("Standardizing size")

    df["employee_range"] = df["size"]

    df["size_category"] = df["size"].apply(normalize_size)

    return df
