from pathlib import Path
import pandas as pd
from typing import List, Optional


def sample_parquet_folder(
    folder_path: str,
    n_files: int = 3,
    columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Read a small sample from the first n Parquet files in a folder.

    Args:
        folder_path (str): Path to folder containing Parquet files.
        n_files (int): Number of files to read for sampling.
        columns (list, optional): Columns to read. Reads all if None.

    Returns:
        pd.DataFrame: Sample DataFrame from first few files.
    """
    folder = Path(folder_path)

    dfs = []

    for i, file in enumerate(sorted(folder.glob("*.parquet"))):

        dfs.append(pd.read_parquet(file, columns=columns, engine="pyarrow"))

        if i + 1 >= n_files:
            break
    return pd.concat(dfs, ignore_index=True)
