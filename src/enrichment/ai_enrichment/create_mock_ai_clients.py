from config.config import get_ai_api_key
from config.logger_config import logger

from src.enrichment.ai_enrichment.ai_client import MockAIClient, AIClient


def create_mock_ai_client() -> MockAIClient:
    logger.info("Using MockAIClient")
    return MockAIClient()


def create_ai_client() -> AIClient:
    return AIClient(
        api_key=get_ai_api_key()
    )
