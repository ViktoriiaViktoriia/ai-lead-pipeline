import pandas as pd
from typing import Optional
from pathlib import Path
from typing import Union

from config.logger_config import logger
from src.schemas import prepare_schema
from src.ingestion.metadata import generate_company_id, get_ingestion_timestamp
from src.processing.data_quality import profile_dataset
from src.utils.validators import validate_file_path, validate_folder_path
from src.utils.helpers import sample_parquet_folder


def load_csv_data(file_path: str, encoding: Optional[str] = "utf-8") -> pd.DataFrame:
    """
    This function loads a CSV dataset from the given file path into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file
        encoding (str, optional): File encoding (default: utf-8)

    Returns:
        pd.DataFrame: Loaded dataset

    Raises:
        FileNotFoundError: If file does not exist
        pd.errors.ParserError: If CSV is malformed
        Exception: For unexpected errors
    """

    # Load CSV
    logger.info("Loading CSV has started.")
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        logger.info(f"Data loaded successfully from {file_path}")
        logger.info(f"Dataset shape: {df.shape}")

    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error: {e}")
        raise

    except UnicodeDecodeError as e:
        logger.error(f"Encoding error: {e}")
        raise

    return df


def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes completely empty rows.
    """
    return df.dropna(how="all")


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds pipeline metadata columns.
    """

    df["company_id"] = generate_company_id(len(df))
    df["ingestion_timestamp"] = get_ingestion_timestamp()

    return df


def process_leads_in_chunks(input_path, output_path, chunk_size=50000):
    """
        Process large CSV file in chunks and save cleaned output as parquet files.
    """

    first_chunk = True
    chunk_number = 0

    for chunk in pd.read_csv(input_path, chunksize=chunk_size):

        try:
            # Shows rows and columns
            logger.info(f"Processing chunk: #{chunk_number + 1} with shape: {chunk.shape}")

            # Apply pipeline steps
            chunk = prepare_schema(chunk)
            chunk = drop_empty_rows(chunk)
            chunk = add_metadata(chunk)

            # Profile the first chunk
            if first_chunk:
                profile_dataset(chunk, logger)
                logger.info(f"First 5 rows of this chunk:\n{chunk.head().to_dict(orient='records')}")

            # Save result
            chunk.to_parquet(
                output_path / f"chunk_{chunk_number:04d}.parquet",
                engine="pyarrow",
                index=False
            )

            chunk_number += 1
            first_chunk = False

        except Exception as e:
            logger.error(f"Failed processing chunk #{chunk_number}: {e}")
            continue

    logger.info("Finished processing large CSV")


def load_raw_leads(input_path: Union[str, Path], output_path: Union[str, Path]) -> None:
    """
    Load raw company leads dataset from CSV, process it in chunks,
    and store the result as partitioned Parquet files.

    Args:
        input_path (str): Path to the raw CSV dataset.
        output_path (str): Directory where processed Parquet files
                           (chunked) will be saved.

    Notes:
        - The output is stored as multiple Parquet files (one per chunk),
          not a single file.
        - This approach is used to handle large datasets efficiently
          without loading all data into memory.
    """

    # Input path
    input_path = Path(input_path)
    logger.info("Input path validation started.")

    # Validate input path before processing
    validate_file_path(input_path)

    # Output path (check folder exists)
    output_path = Path(output_path)
    if not output_path.parent.exists():
        logger.error(f"Output directory does not exist: {output_path.parent}")
        raise FileNotFoundError(f"Output directory does not exist: {output_path.parent}")
    logger.info(f"Output path is ready: {output_path}")

    # Process dataset in chunks
    process_leads_in_chunks(input_path, output_path)

    logger.info(f"Raw leads have been processed and saved to {output_path}")

    # Validate output path after processing
    logger.info("Validating output path.")
    validate_folder_path(output_path)

    logger.info(f"Output chunk folder successfully created: {output_path}")

    sample_df = sample_parquet_folder(output_path)
    logger.info(f"Sample from first 3 chunks:\n{sample_df.head().to_dict(orient='records')}")
