from unittest.mock import Mock

from src.enrichment.run_mode import (should_process_row, normalize_data,
                                     call_api_with_retry, handle_run_mode)


# Test handle_run_mode()
def test_handle_run_mode_dry(set_mock_run_mode):
    with set_mock_run_mode("dry"):
        result = handle_run_mode("test.com", 0, 10)
        assert result == "continue"


def test_handle_run_mode_mock(set_mock_run_mode):
    with set_mock_run_mode("mock"):
        result = handle_run_mode("test.com", 0, 10)
        assert result == "mock"


def test_handle_run_mode_limited_below_limit(set_mock_run_mode):
    with set_mock_run_mode("limited"):
        result = handle_run_mode("test.com", 5, 10)
        assert result == "proceed"


def test_handle_run_mode_full(set_mock_run_mode):
    with set_mock_run_mode("full"):
        result = handle_run_mode("test.com", 10, 10)
        assert result == "break"


# Test should_process_row()
def test_should_process_valid(seen_domains):
    assert should_process_row("example.com", seen_domains) is True


def test_should_skip_no_domain(seen_domains):
    assert should_process_row(None, seen_domains) is False


def test_should_skip_seen_domain(seen_domains):
    seen_domains.add("example.com")
    assert should_process_row("example.com", seen_domains) is False


# Test call_api_with_retry()
def test_call_api_success(mock_abstract_client):
    result = call_api_with_retry(mock_abstract_client, "test.com", "abstract")
    assert result["company_name"] == "Test"


def test_call_api_failure_returns_none():
    client = Mock()
    client.enrich_company.side_effect = Exception("API error")
    result = call_api_with_retry(client, "test.com", "abstract")
    assert result is None


# Test normalize_data()
def test_normalize_data_merges_correctly(sample_df_small):
    row = sample_df_small.iloc[0]
    api_data = {"company_name": "New", "industry": "Tech"}

    result = normalize_data(row, api_data, "abstract")

    assert result["company_name"] == "New"
    assert result["industry"] == "Tech"
    assert result["source"] == "abstract"


def test_api_does_not_overwrite_existing_with_none(sample_df_mid):
    row = sample_df_mid.iloc[0]

    api_data = {"company_name": None}

    result = normalize_data(row, api_data, "abstract")

    assert result["company_name"] == "Existing Co"


def test_partial_api_enrichment(sample_df_mid):
    row = sample_df_mid.iloc[0]

    api_data = {
        "company_name": None,
        "industry": "Tech",
        "country": None
    }

    result = normalize_data(row, api_data, "abstract")

    assert result["company_name"] == "Existing Co"  # preserved
    assert result["industry"] == "Tech"  # added
    assert "country" not in result or result["country"] is None


def test_no_data_loss(sample_df_mid):
    row = sample_df_mid.iloc[0]

    api_data = {}

    result = normalize_data(row, api_data, "abstract")

    assert result["domain"] == "a.com"
    assert result["company_name"] == "Existing Co"
