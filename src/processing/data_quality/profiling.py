def profile_dataset(df, logger):
    """
    Generate and log basic dataset profiling information.

    Includes:
    - dataset shape
    - column names
    - missing values
    - empty rows
    """

    logger.info("Dataset profiling started")

    # Shape
    logger.info(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    # Columns
    logger.info(f"Columns: {df.columns.tolist()}")

    # Missing values
    missing = df.isnull().sum()
    logger.info(f"Missing values:\n{missing}")

    # Empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    logger.info(f"Empty rows: {empty_rows}")
