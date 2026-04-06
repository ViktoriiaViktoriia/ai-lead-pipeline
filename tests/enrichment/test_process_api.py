from unittest.mock import patch

from src.enrichment.process_api import process_api_batch


@patch("src.enrichment.process_api.handle_run_mode", return_value="proceed")
# Test process_api_batch()
def test_process_api_batch_success(
    mock_run_mode,
    sample_df_small,
    mock_abstract_client,
    seen_domains
):
    rows, seen, calls = process_api_batch(
        df=sample_df_small,
        client=mock_abstract_client,
        source_name="abstract",
        required_fields=mock_abstract_client.REQUIRED_FIELDS,
        call_limit=10,
        seen_domains=seen_domains,
        mode="full"
    )

    assert len(rows) == len(sample_df_small)
    assert calls == len(sample_df_small)

    # Client was called
    assert mock_abstract_client.enrich_company.call_count == len(sample_df_small)
    assert rows[0]["domain"] == "a.com"

    # Seen tracking
    assert "a.com" in seen
    assert "b.com" in seen

    # Enrichment structure
    assert all("source" in row for row in rows)


@patch("src.enrichment.process_api.handle_run_mode", return_value="proceed")
def test_process_api_batch_respects_call_limit(
    mock_run_mode,
    sample_df_small,
    mock_abstract_client,
    seen_domains
):
    rows, seen, calls = process_api_batch(
        df=sample_df_small,
        client=mock_abstract_client,
        source_name="abstract",
        required_fields=mock_abstract_client.REQUIRED_FIELDS,
        call_limit=1,
        seen_domains=seen_domains,
        mode="limited"
    )

    assert calls == 1
    assert len(rows) == 1


@patch("src.enrichment.process_api.handle_run_mode", return_value="proceed")
def test_process_api_batch_skips_seen_domains(
    mock_run_mode,
    sample_df_small,
    mock_abstract_client
):
    seen_domains = {"a.com", "b.com"}

    rows, seen, calls = process_api_batch(
        df=sample_df_small,
        client=mock_abstract_client,
        source_name="abstract",
        required_fields=mock_abstract_client.REQUIRED_FIELDS,
        call_limit=1,
        seen_domains=seen_domains,
        mode="limited"
    )

    assert "a.com" in seen
    assert calls < len(sample_df_small)
