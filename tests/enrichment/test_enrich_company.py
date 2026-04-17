from unittest.mock import patch, Mock
import pandas as pd

from src.enrichment.enrich_company import enrich_company_chunk, enrich_company_parquet


# Test enrich_company_chunk()
@patch("src.enrichment.enrich_company.process_api_batch")
def test_enrich_company_chunk(mock_process, sample_df_large, seen_domains):
    # Mock return values for Abstract and TechnologyChecker
    mock_process.side_effect = [
        ([{
            "domain": "a.com",
            "company_name": "A Inc",
            "industry": "Tech",
            "country": "US",
            "source": "abstract_test"
        }], {"a.com"}, 1),

        ([{
            "domain": "b.com",
            "company_name": "B Ltd",
            "industry": "IT",
            "country": "UK",
            "source": "technologychecker_test"
        }], {"a.com", "b.com"}, 1),
    ]

    result_df, seen = enrich_company_chunk(
        df_abstract=sample_df_large,
        df_tech=sample_df_large,
        seen_domains=seen_domains,
        mode="full",
        abstract_client=Mock(),
        tech_client=Mock(),
        call_limit=1,
    )
    result_df = pd.DataFrame(result_df)

    print(result_df)
    print(type(result_df))

    assert len(result_df) == 2
    assert "a.com" in seen
    assert "b.com" in seen
    assert set(result_df["source"]) == {"abstract_test", "technologychecker_test"}


@patch("src.enrichment.enrich_company.process_api_batch")
def test_enrich_company_chunk_integration(
    mock_process_api_batch,
    sample_df_large,
):
    chunk = sample_df_large

    seen_domains = set()

    df_top = chunk.iloc[:2]

    # Mock API batch (Abstract + Tech)
    mock_process_api_batch.side_effect = [
        # Abstract result
        (
            [
                {"domain": "a.com", "source": "abstract_test"},
                {"domain": "c.com", "source": "abstract_test"},
            ],
            {"a.com", "c.com"},
            2
        ),
        # TechnologyChecker result
        (
            [
                {"domain": "d.com", "source": "tech"},
            ],
            {"a.com", "c.com", "d.com"},
            1
        )
    ]

    # Mock clients
    mock_abstract_client = Mock()
    mock_abstract_client.abstract_required_fields = ["company_name", "industry", "country"]

    mock_tech_client = Mock()
    mock_tech_client.tech_required_fields = ["company_name", "industry", "country"]

    # Run function
    result_df, seen = enrich_company_chunk(
        df_abstract=df_top,
        df_tech=df_top,
        seen_domains=seen_domains,
        mode="full",
        abstract_client=mock_abstract_client,
        tech_client=mock_tech_client,
        call_limit=1,
    )
    result_df = pd.DataFrame(result_df)

    assert not result_df.empty
    assert "source" in result_df.columns
    assert len(seen) == 3


# Test enrich_company_parquet()
@patch("src.enrichment.enrich_company.enrich_company_chunk")
def test_enrich_company_parquet(mock_enrich_chunk, tmp_path, sample_df_large):

    input_dir = tmp_path / "input"
    input_dir.mkdir()

    output_dir = tmp_path / "output"
    seen_file = tmp_path / "seen.csv"

    df_input = sample_df_large

    (input_dir / "test.parquet").write_bytes(df_input.to_parquet())

    mock_enrich_chunk.return_value = (
        [{"domain": "a.com", "source": "abstract_test"}],
        {"a.com"}
    )

    enrich_company_parquet(
        input_dir,
        output_dir,
        seen_file,
        mode="full",
        abstract_client=Mock(),
        tech_client=Mock()
    )

    output_files = list(output_dir.glob("*.csv"))
    assert len(output_files) == 1

    result_df = pd.read_csv(output_files[0])
    assert not result_df.empty

    assert seen_file.exists()

    assert mock_enrich_chunk.called


# Test empty case
@patch("src.enrichment.enrich_company.enrich_company_chunk")
def test_enrich_company_parquet_empty(mock_enrich_chunk, tmp_path, sample_df_large):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    output_dir = tmp_path / "output"
    seen_file = tmp_path / "seen.csv"

    df_input = sample_df_large

    file_path = input_dir / "test.parquet"
    df_input.to_parquet(file_path)

    mock_enrich_chunk.return_value = ([], set())

    mock_abstract_client = Mock()
    mock_tech_client = Mock()

    # Run function
    enrich_company_parquet(
        input_dir,
        output_dir,
        seen_file,
        mode="full",
        abstract_client=mock_abstract_client,
        tech_client=mock_tech_client
    )

    assert not list(output_dir.glob("*.parquet"))
