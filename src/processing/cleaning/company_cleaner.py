import pandas as pd
import re
from typing import Optional

from config.logger_config import logger
from config.variables import LEGAL_SUFFIXES


def normalize_company_name(name: str) -> Optional[str]:
    """
    Normalize company name: lowercase, remove special characters, remove legal suffixes.
    """
    # Check name value
    if not isinstance(name, str):
        return None

    if pd.isna(name):
        return None

    try:
        name = str(name)
    except AttributeError:
        return None

    name = name.lower().strip()

    # Remove punctuation
    name = re.sub(r"[^\w\s]", "", name)

    # Remove extra spaces
    name = re.sub(r"\s+", " ", name)

    # Remove legal suffixes
    words = name.split()
    words = [w for w in words if w not in LEGAL_SUFFIXES]

    return " ".join(words)


def clean_company_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply company name normalization.
    """

    # Count missing before
    missing_before = df["company_name"].isna().sum()

    # Raw company names column
    df["company_name_raw"] = df["company_name"]

    # Clean company names
    logger.info("Normalizing company names")
    df["company_name"] = df["company_name"].apply(normalize_company_name)

    # Count missing after
    missing_after = df["company_name"].isna().sum()

    logger.info(
        f"missing before: {missing_before} -> after: {missing_after}"
    )

    logger.debug(
        df[["company_name_raw", "company_name"]]
        .head(5)
        .to_dict(orient="records")
    )

    return df
