from pathlib import Path
import pandas as pd
from contextlib import contextmanager
import time
from typing import List, Optional
from typing import Union


def sample_parquet_folder(
    folder_path: Union[str, Path],
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


@contextmanager
def rate_limited(rate_per_sec: float = 1.0):
    """
    Context manager to enforce API rate limiting. Wraps an API call and ensures that execution
    respects a specified request rate by automatically applying delay after the operation.

    Args:
        rate_per_sec (float): Maximum number of allowed requests per second.

    Behavior:
        - Measures execution time of the wrapped block.
        - Sleeps for the remaining time needed to maintain the rate limit.
    """

    start_time = time.time()
    yield
    elapsed = time.time() - start_time
    sleep_time = max(0.0, (1.0 / rate_per_sec) - elapsed)
    time.sleep(sleep_time)

