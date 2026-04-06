import os
from dotenv import load_dotenv
from pathlib import Path

# loads .env into environment
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

RAW_DATA_PATH = DATA_DIR / "companies_raw_2023_q4.csv"

CHUNKED_DATA_PATH = DATA_DIR / "processed" / "chunked"

CLEANED_DATA_PATH = DATA_DIR / "processed" / "cleaned"

API_ENRICHED_DATA_PATH = DATA_DIR / "enriched" / "api_enriched_data"

SEEN_DOMAINS_PATH = DATA_DIR / "enriched" / "api_enriched_data" / "seen_domains"

AI_ENRICHED_DATA_PATH = DATA_DIR / "enriched" / "ai_enriched_data"


# Abstract company enrichment API
BASE_URL_ABSTRACT = os.getenv("BASE_URL_ABSTRACT")
PRIMARY_API_KEY_ABSTRACT = os.getenv("PRIMARY_API_KEY_ABSTRACT")


# Validation only when key is actually used
def get_primary_api_key_abstract() -> str:
    value = os.getenv("PRIMARY_API_KEY_ABSTRACT")
    if not value:
        raise ValueError("PRIMARY_API_KEY_ABSTRACT is missing in .env")
    return value


# Technologychecker company enrichment API
BASE_URL_TECHNOLOGYCHEKER = os.getenv("BASE_URL_TECHNOLOGYCHEKER")
API_TOKEN_TECHNOLOGYCHEKER = os.getenv("API_TOKEN_TECHNOLOGYCHEKER")


def get_api_token_technology() -> str:
    value = os.getenv("API_TOKEN_TECHNOLOGYCHEKER")
    if not value:
        raise ValueError("API_TOKEN_TECHNOLOGYCHEKER is missing in .env")
    return value
