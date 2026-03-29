"""
Defines dataset schemas used in the data pipeline.
These are NOT database models.
"""

BASE_COLUMNS = [
    "company_id",
    "company_name",
    "domain",
    "industry",
    "size",
    "country",
    "is_valid_domain",
    "domain_source",
    "ingestion_timestamp"
]

ENRICHED_COLUMNS = BASE_COLUMNS + [
    "email",
    "phone",
    "contact_name",
    "job_title",
    "enrichment_source",
    "confidence_score"
]