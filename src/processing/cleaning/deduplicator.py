import pandas as pd

from config.logger_config import logger


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate rows using a composite key.
    """

    before = len(df)

    # Create key
    df["deduplication_key"] = (
        df["company_name"].fillna("") + "_" +
        df["domain"].fillna("")
    )

    # Drop duplicates
    df = df.drop_duplicates(subset=["deduplication_key"], keep="first")

    after = len(df)

    logger.info(f"Deduplication removed {before - after} rows")

    # Drop helper column
    df = df.drop(columns=["deduplication_key"])

    return df
