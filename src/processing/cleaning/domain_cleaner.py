import pandas as pd
import tldextract
import re
from typing import Optional

from config.logger_config import logger


def normalize_domain(domain: str) -> Optional[str]:
    """
    Normalize domain by removing protocol, www, and lowercasing.
    """

    if pd.isna(domain):
        return None

    domain = domain.strip().lower()

    # Remove protocol
    domain = re.sub(r"^https?://", "", domain)

    # Remove www
    domain = re.sub(r"^www\.", "", domain)

    return domain


def is_valid_domain(domain: str) -> bool:
    """
    Validate domain using tldextract.
    """

    if not domain:
        return False

    extracted = tldextract.extract(domain)

    # Valid if domain and suffix exist
    return bool(extracted.domain and extracted.suffix)


def detect_domain_source(domain: str) -> Optional[str]:
    """
    Identify source of domain.
    """

    if pd.isna(domain) or domain == "":
        return None

    return "raw"


def clean_domain(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich domain column.
    """
    total_rows = len(df)

    # Preserve raw
    df["website_raw"] = df["domain"]

    # Normalize into domain
    logger.info("Validating and normalizing domains")
    df["domain"] = df["domain"].apply(normalize_domain)

    df["is_valid_domain"] = df["domain"].apply(is_valid_domain)

    valid_count = df["is_valid_domain"].sum()
    invalid_count = total_rows - valid_count
    logger.info(f"valid domain count: {valid_count} | invalid domain count: {invalid_count}")

    df["domain_source"] = df["domain"].apply(detect_domain_source)
    count_domain_source = df["domain_source"].value_counts().to_dict()
    logger.info(f"Domain sources detected:\n{count_domain_source}")

    return df
