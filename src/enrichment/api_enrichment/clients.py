from config.config import (get_primary_api_key_abstract, BASE_URL_ABSTRACT,
                           BASE_URL_TECHNOLOGYCHEKER, get_api_token_technology)

from src.enrichment.api_enrichment.abstract_client import AbstractClient
from src.enrichment.api_enrichment.technologychecker_client import TechnologyCheckerClient


def create_abstract_client() -> AbstractClient:
    return AbstractClient(
        api_key=get_primary_api_key_abstract(),
        base_url=BASE_URL_ABSTRACT
    )


def create_tech_client() -> TechnologyCheckerClient:
    return TechnologyCheckerClient(
        api_key=get_api_token_technology(),
        base_url=BASE_URL_TECHNOLOGYCHEKER
    )
