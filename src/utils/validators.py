from pathlib import Path
from typing import Union, Sequence
import pandas as pd

from config.logger_config import logger
from config.variables import RUN_MODE


def validate_folder_path(folder_path: Union[str, Path]) -> None:
    """
    Ensure folder exists; if not, create it.
    """
    folder = Path(folder_path)
    if not folder.exists():
        logger.info(f"Folder does not exist, creating: {folder}")
        folder.mkdir(parents=True, exist_ok=True)
    elif not folder.is_dir():
        raise ValueError(f"Path exists but is not a folder: {folder}")
    else:
        logger.info(f"Folder validation completed: {folder}")


def validate_file_path(file_path: Union[str, Path]) -> None:
    """
    Validate that the given file path exists and points to a file.

    Args:
        file_path (str): Path to the file

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If path exists but is not a file
    """
    path = Path(file_path)

    if not path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    if not path.is_file():
        logger.error(f"Path is not a file: {file_path}")
        raise ValueError(f"Path is not a file: {file_path}")

    if path.suffix.lower() != ".csv":
        logger.warning(f"Expected CSV file, got: {path.suffix}")

    if path.stat().st_size == 0:
        logger.warning(f"File is empty: {file_path}")

    logger.info(f"File validation passed: {file_path}")


def validate_required_columns(df: pd.DataFrame, required_columns: list):
    """
    Ensures required columns exist in dataset.
    """
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_api_data(api_data: dict, required_fields: Sequence[str]) -> bool:
    """
        Validate API response data.

        Ensures that required fields are present in the response before
        processing or storing the data.

        Args:
            api_data (dict): Response dictionary from the API.
            required_fields (Sequence[str]): String or list/tuple, etc. of required fields to validate.

        Returns:
            bool: True if all required fields are present, False otherwise.

        """
    return all(field in api_data for field in required_fields)


def validate_run_mode():
    """
    Validate run mode. Requires conformation in case of FULL RUN.
    """
    allowed = {"dry", "mock", "limited", "full"}

    if RUN_MODE not in allowed:
        raise ValueError(f"Invalid RUN_MODE: {RUN_MODE}")

    logger.info(f"Running in mode: {RUN_MODE}")

    if RUN_MODE == "full":
        confirm = input(" ! FULL RUN will consume API credits. Type 'YES' to continue: ")
        if confirm != "YES":
            raise RuntimeError("Full run cancelled by user.")
