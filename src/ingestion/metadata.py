import uuid
from datetime import datetime
from typing import List


def generate_company_id(n: int) -> List[str]:
    """
    Generate unique IDs for dataset rows.

    Args:
        n (int): Number of IDs to generate

    Returns:
        List[str]: List of UUID strings
    """
    return [str(uuid.uuid4()) for _ in range(n)]


def get_ingestion_timestamp():
    """
    Get current UTC timestamp for ingestion.

    Returns:
        datetime: UTC timestamp
    """
    return datetime.utcnow()
