import pandas as pd
from pathlib import Path
from typing import Union

from config.logger_config import logger
from src.processing.cleaning.company_cleaner import clean_company_name
from src.processing.cleaning.size_cleaner import clean_size
from src.processing.cleaning.deduplicator import deduplicate
from src.processing.cleaning.domain_cleaner import clean_domain
from src.processing.cleaning.industry_cleaner import clean_industry
from src.processing.cleaning.location_cleaner import clean_location
from src.processing.data_quality import profile_dataset


def clean_data(input_path: Union[str, Path], output_path: Union[str, Path]) -> None:
    """
    Clean dataset stored as multiple Parquet files (chunked).

    Args:
        input_path: folder containing raw parquet chunks
        output_path: folder to save cleaned parquet chunks
    """

    input_path = Path(input_path)
    output_path = Path(output_path)

    logger.info(f"Starting data cleaning for folder: {input_path}")

    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(input_path.glob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No parquet files found in {input_path}")
        return

    first_chunk = True

    for chunk_number, file in enumerate(parquet_files):

        logger.info(f"Processing file {chunk_number}: {file.name}")

        try:
            # Read ONE parquet file (acts as chunk)
            chunk = pd.read_parquet(file, engine="pyarrow")
        except Exception as e:
            logger.error(f"Failed to read {file}: {e}")
            continue

        logger.info(f"Chunk size: {len(chunk)} rows")
        logger.info(f"Initial missing values:\n{chunk.isna().sum().to_dict()}")

        try:
            # Cleaning steps
            chunk = clean_company_name(chunk)
            chunk = clean_domain(chunk)
            chunk = clean_size(chunk)
            chunk = clean_industry(chunk)
            chunk = clean_location(chunk)
            chunk = deduplicate(chunk)

        except Exception as e:
            logger.error(f"Failed to process {file}: {e}")
            continue

        # Profile only first chunk
        if first_chunk:
            profile_dataset(chunk, logger)
            logger.info(
                f"First 5 rows sample:\n"
                f"{chunk.head().to_dict(orient='records')}"
            )
            first_chunk = False

        # Save cleaned chunk
        output_file = output_path / f"cleaned_{chunk_number:04d}.parquet"

        try:
            chunk.to_parquet(
                output_file,
                engine="pyarrow",
                index=False
            )
            logger.info(f"Saved cleaned chunk: {output_file}")

        except Exception as e:
            logger.error(f"Failed to save {output_file}: {e}")
            continue

        del chunk

    logger.info(f"Data cleaning completed for folder: {input_path}")
    logger.info(f"Cleaned data stored in: {output_path}")
