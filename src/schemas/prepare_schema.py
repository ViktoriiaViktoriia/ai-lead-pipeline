import pandas as pd

from config.logger_config import logger
from src.schemas import BASE_COLUMNS
from src.utils.validators import validate_required_columns


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    The function standardizes column names (lowercase, strip spaces, map column names).
    Renames columns into standardized schema.

    """

    cleaned_columns = []

    for i, col in enumerate(df.columns):

        # Handle None or empty strings
        if not col or col.strip() == "":
            new_col = f"unknown_{i}"

        else:
            col = col.strip().lower()

            # Handle "Unnamed: X"
            if "unnamed" in col:
                new_col = f"unknown_{i}"
            else:
                new_col = col

        cleaned_columns.append(new_col)

    df.columns = cleaned_columns

    column_mapping = {
        "company": "company_name",
        "name": "company_name",
        "website": "domain",
        "domain": "domain",
        "industry": "industry",
        "size": "size",
        "country": "country",
        "country-code": "country",
        "country_code": "country"
    }

    df = df.rename(columns=column_mapping)

    return df


def select_base_columns(df: pd.DataFrame, base_columns: list) -> pd.DataFrame:
    """
    The function selects relevant (base) columns for pipeline.
    """

    relevant_columns = [col for col in base_columns if col in df.columns]
    return df[relevant_columns]


def prepare_schema(df):
    df = standardize_column_names(df)

    validate_required_columns(df, ["company_name"])

    df = select_base_columns(df, BASE_COLUMNS)

    logger.info(f"Columns after standardization: {df.columns.tolist()}")

    return df
