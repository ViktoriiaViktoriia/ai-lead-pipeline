import pandas as pd
import re

from config.logger_config import logger


# Common legal suffixes across countries
LEGAL_SUFFIXES = [
    "llc", "inc", "ltd", "gmbh", "oy", "oyj", "ab", "as",
    "bv", "sa", "sarl", "plc", "limited", "corporation", "corp"
]


def normalize_company_name(name: str) -> str | None:
    """
    Normalize company name: lowercase, remove special characters, remove legal suffixes.
    """

    if pd.isna(name):
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
    total_rows = len(df)

    # Count missing before
    missing_before = df["company_name"].isna().sum()

    # Raw company names column
    df["company_name_raw"] = df["company_name"]

    # Clean company names
    df["company_name"] = df["company_name"].apply(normalize_company_name)

    # Count missing after
    missing_after = df["company_name"].isna.sum()

    logger.info(
        f"Company cleaning: {total_rows} rows processed | "
        f"missing before: {missing_before} -> after: {missing_after}"
    )

    logger.debug(
        df[["company_name_raw", "company_name"]]
        .head(5)
        .to_dict(orient="records")
    )

    return df
