from unittest.mock import MagicMock, patch

from src.enrichment.ai_enrichment.ai_enrichment import ai_enrich
from src.enrichment.ai_enrichment.ai_client import MockAIClient


@patch("src.enrichment.ai_enrichment.ai_enrichment.build_prompt")
def test_ai_enrich_returns_empty_on_exception(mock_build_prompt):

    mock_build_prompt.side_effect = Exception("Boom")

    row = {
        "domain": "a.com",
        "company_name": "Company A"
    }

    client = MagicMock()

    result = ai_enrich(row, client)

    assert result == {}


@patch("src.enrichment.ai_enrichment.ai_enrichment.validate_ai_output")
@patch("src.enrichment.ai_enrichment.ai_enrichment.parse_response")
@patch("src.enrichment.ai_enrichment.ai_enrichment.build_prompt")
def test_ai_enrich_success(
    mock_build_prompt,
    mock_parse_response,
    mock_validate
):
    row = {
        "domain": "a.com",
        "company_name": "Company A"
    }

    mock_build_prompt.return_value = "prompt"

    mock_parse_response.return_value = {
        "summary": "test"
    }

    mock_validate.return_value = {
        "summary": "test",
        "source": "ai:gpt"
    }

    client = MagicMock()
    client.model = "gpt"
    client.generate.return_value = "raw response"

    result = ai_enrich(row, client)

    assert result["summary"] == "test"

    mock_build_prompt.assert_called_once()
    client.generate.assert_called_once()
    mock_parse_response.assert_called_once()
    mock_validate.assert_called_once()


def test_ai_enrich_returns_mock_response():
    row = {
        "domain": "a.com",
        "company_name": "Company A"
    }

    client = MockAIClient()

    result = ai_enrich(row, client)

    assert result["ai_summary"] == "mocked value"
    assert result["ai_score"] == 0.0
    assert result["source"] == "ai:mock"


def test_ai_enrich_returns_empty_for_missing_identifiers():
    row = {}

    result = ai_enrich(row, ai_client=None)

    assert result == {}
