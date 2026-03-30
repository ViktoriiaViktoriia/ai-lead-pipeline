from src.ingestion.load_company_leads import load_raw_leads
from config.config import RAW_DATA_PATH, CHUNKED_DATA_PATH


def run_pipeline():

    # Load raw data
    load_raw_leads(RAW_DATA_PATH, CHUNKED_DATA_PATH)


if __name__ == "__main__":
    run_pipeline()
