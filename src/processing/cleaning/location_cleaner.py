import pandas as pd
from typing import Optional

from config.logger_config import logger
from config.variables import COUNTRY_MAPPING


def normalize_country(country: str) -> Optional[str]:
    """
    Normalize country values.
    """

    if pd.isna(country):
        return None

    country = country.strip().lower()

    return COUNTRY_MAPPING.get(country, country)


def clean_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize location fields.
    """

    logger.info("Standardizing location")

    df["country_raw"] = df["country"]
    df["country"] = df["country"].apply(normalize_country)

    return df
