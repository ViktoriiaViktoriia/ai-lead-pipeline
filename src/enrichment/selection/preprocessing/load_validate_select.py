import pandas as pd
from pathlib import Path

from config.logger_config import logger

from src.ingestion.load_company_leads import load_csv_data
from src.processing.cleaning.industry_cleaner import clean_industry
from src.enrichment.selection.lead_prioritizer import select_top_leads
from src.processing.data_quality.profiling import profile_dataset
from src.utils.validators import validate_file_path


def load_data(path: str) -> pd.DataFrame:
    """
    Loading raw company data from storage

    Args:
         path(str): Input path to dataset
    Return: Dataframe
    """

    # Input path
    input_path = Path(path)
    logger.info("Input path validation started.")

    # Validate input path before processing
    validate_file_path(input_path)

    logger.info(f"Loading data from path: {path}")

    loaded_df = load_csv_data(path)

    profile_dataset(loaded_df, logger)

    return loaded_df


def basic_filtering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applying basic data validation and filtering

    Args:
         df (pd.DataFrame): Input dataset

    Return:
        pd.DataFrame: Cleaned dataset
    """
    logger.info("Applying basic filtering")

    df = clean_industry(df)

    required_columns = {"domain", "company_name", "industry", "country"}

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()

    domain_valid = df["domain"].astype(str).str.strip().ne("")
    company_valid = df["company_name"].astype(str).str.strip().ne("")
    industry_valid = df["industry"].astype(str).str.strip().ne("")
    country_valid = df["country"].astype(str).str.strip().ne("")
    df = df[domain_valid | company_valid | industry_valid | country_valid]

    # domain sanity
    df = df[df["domain"].astype(str).str.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", na=False)]

    # company name sanity
    df = df[df["company_name"].astype(str).str.len() > 2]

    # fill industry safely
    df["industry"] = df["industry"].fillna("unknown")

    logger.info(f"Remaining rows after filtering: {len(df)}")

    profile_dataset(df, logger)

    return df


def select_candidates(
        df: pd.DataFrame,
        top_n: int = 20000,
        min_score: int = 35
) -> pd.DataFrame:
    """
    Computing preliminary priority scores. Selecting high-quality candidates for AI enrichment.

    The goal is to ensure that only clean, relevant, and high-priority records are sent to the AI model,
    reducing cost and improving overall data quality.

    Args:
        df (pd.DataFrame): Input dataset containing precomputed scores.
        top_n (int): Maximum number of records to select.
        min_score (int): Minimum score required for a record to be included.

    Returns:
        pd.DataFrame: A filtered copy of the dataset containing only
        records that meet the score threshold, sorted by score in descending order.
    """
    logger.info("Selecting top candidates.")

    df_top = select_top_leads(df, limit=top_n)

    df_top = df_top[df_top["priority_score"] >= min_score]

    df_top = df_top.drop_duplicates(subset=["domain"], keep="first")

    profile_dataset(df_top, logger)

    logger.info(f"Leads selection for AI enrichment completed.")

    return df_top.copy()
