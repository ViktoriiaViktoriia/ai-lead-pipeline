from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

RAW_DATA_PATH = DATA_DIR / "companies_raw_2023_q4.csv"

CHUNKED_DATA_PATH = DATA_DIR / "processed" / "chunk_company_leads_2023_q4.csv"
