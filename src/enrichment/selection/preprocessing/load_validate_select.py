import pandas as pd
from pathlib import Path

from config.logger_config import logger

from src.ingestion.load_company_leads import load_csv_data
from src.enrichment.ai_enrichment.ai_enrichment import ai_enrich
from src.enrichment.rule_based_enrichment import rule_based_enrich
from src.enrichment.routing import Route, assign_routes
from src.enrichment.selection.lead_prioritizer import select_top_leads
from src.enrichment.selection.preprocessing.normalize_size import normalize_employee_range, assign_size_category
from src.processing.data_quality.profiling import profile_dataset
from src.processing.cleaning.industry_cleaner import clean_industry
from src.processing.cleaning.location_cleaner import clean_location
from src.processing.cleaning.company_cleaner import clean_company_name
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

    df = clean_company_name(df)
    df = clean_industry(df)
    df = clean_location(df)

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

    # normalize employee range
    df["employee_range_normalized"] = df.apply(normalize_employee_range, axis=1)

    # update size category
    df["size_category"] = df.apply(assign_size_category, axis=1)

    logger.info(f"Remaining rows after filtering: {len(df)}")

    profile_dataset(df, logger)

    return df


def select_candidates(
        df: pd.DataFrame,
        top_n: int = 20000,
        min_score: int = 35
) -> pd.DataFrame:
    """
    Computing preliminary priority scores. Selecting high-quality candidates for rule-based enrichment
    and AI enrichment.

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

    logger.info(f"Leads selection for enrichment completed.")

    return df_top.copy()


def process_rows(df, ai_client):
    """
    Process dataset rows by applying routing logic and enrichment.

    Workflow:
        1. Assign routes to each row (AI, rule-based, or skip)
        2. Skip invalid rows
        3. Apply rule-based enrichment to all valid rows
        4. Apply AI enrichment only to selected rows (based on routing)
        5. Merge enrichment results into a unified output

    Args:
        df (pd.DataFrame): Input dataset containing company information
        ai_client: Initialized AI client used for enrichment

    Returns:
        List[Dict[str, Any]]: List of enriched records
    """
    df = assign_routes(df)

    results = []
    skip_count = 0

    records = df.to_dict(orient="records")

    for row in records:
        route = row.get("route")

        # Skip invalid rows
        if route == Route.SKIP.value:
            skip_count += 1
            continue

        # Rule-based enrichment
        base_result = rule_based_enrich(row)

        # AI enrichment
        if route == Route.AI.value:
            ai_result = ai_enrich(row, ai_client)

            if ai_result:
                base_result.update(ai_result)

        results.append(base_result)

    logger.info(
        "Processing completed",
        extra={
            "total_rows": len(df),
            "skipped": skip_count,
            "processed": len(results)
        }
    )

    return results
