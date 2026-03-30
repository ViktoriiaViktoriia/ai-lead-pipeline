import pandas as pd
from typing import Optional
from pathlib import Path

from config.logger_config import logger
from src.schemas import prepare_schema
from src.ingestion.metadata import generate_company_id, get_ingestion_timestamp
from src.processing.data_quality import profile_dataset
from src.utils.validators import validate_file_path


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
        Process large CSV file in chunks and save cleaned output.
    """

    first_chunk = True

    for chunk in pd.read_csv(input_path, chunksize=chunk_size):

        # Shows rows and columns
        logger.info(f"Processing chunk: {chunk.shape}")

        # Apply pipeline steps
        chunk = prepare_schema(chunk)
        chunk = drop_empty_rows(chunk)
        chunk = add_metadata(chunk)

        # Profile the first chunk
        if first_chunk:
            profile_dataset(chunk, logger)
            logger.info(f"First 5 rows of this chunk:\n{chunk.head().to_dict(orient='records')}")

        # Save result
        chunk.to_csv(
            output_path,
            mode="w" if first_chunk else "a",
            index=False,
            header=first_chunk
        )

        first_chunk = False

    logger.info("Finished processing large CSV")


def load_raw_leads(input_path: str, output_path: str) -> None:
    """
    Full ingestion pipeline for raw company leads dataset.

    - Validates input file path.
    - Processes dataset in chunks.
    - Applies basic cleaning and metadata.
    - Saves cleaned chunks to output CSV.

    Args:
        input_path (str or Path): Path to the raw CSV file.
        output_path (str or Path): Path where processed CSV will be saved.
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
    validate_file_path(output_path)

    logger.info(f"Output file successfully created: {output_path}")

    df_head = pd.read_csv(output_path, nrows=5)
    logger.info(f"Output sample:\n{df_head.head().to_dict(orient='records')}")
