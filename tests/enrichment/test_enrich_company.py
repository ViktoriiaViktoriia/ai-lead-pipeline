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
            "source": "abstract"
        }], {"a.com"}, 1),

        ([{
            "domain": "b.com",
            "company_name": "B Ltd",
            "industry": "IT",
            "country": "UK",
            "source": "technologychecker"
        }], {"a.com", "b.com"}, 1),
    ]

    result_df, seen = enrich_company_chunk(
        sample_df_large,
        seen_domains,
        abstract_client=Mock(),
        tech_client=Mock()
    )

    print(result_df)
    print(type(result_df))

    assert len(result_df) == 2
    assert "a.com" in seen
    assert "b.com" in seen
    assert set(result_df["source"]) == {"abstract", "technologychecker"}


@patch("src.enrichment.enrich_company.process_api_batch")
@patch("src.enrichment.enrich_company.select_top_leads")
def test_enrich_company_chunk_integration(
    mock_select_top_leads,
    mock_process_api_batch,
    sample_df_large
):
    chunk = sample_df_large

    seen_domains = set()

    df_top = chunk.iloc[:2]
    df_rest = chunk.iloc[2:]

    mock_select_top_leads.return_value = df_top

    # Mock API batch (Abstract + Tech)
    mock_process_api_batch.side_effect = [
        # Abstract result
        (
            [
                {"domain": "a.com", "source": "abstract"},
                {"domain": "c.com", "source": "abstract"},
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
        chunk,
        seen_domains=seen_domains,
        abstract_client=mock_abstract_client,
        tech_client=mock_tech_client
    )

    assert not result_df.empty
    assert "source" in result_df.columns
    assert len(seen) == 3

    mock_select_top_leads.assert_called_once()
    assert mock_process_api_batch.call_count == 2


# Test enrich_company_parquet()
@patch("src.enrichment.enrich_company.enrich_company_chunk")
def test_enrich_company_parquet(mock_enrich_chunk, tmp_path):

    # Setup folders (pytest creates a temporary folder)
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    output_dir = tmp_path / "output"
    seen_file = tmp_path / "seen.csv"

    # Create fake parquet file
    df_input = pd.DataFrame([
        {"domain": "a.com"},
        {"domain": "b.com"}
    ])

    file_path = input_dir / "test.parquet"
    df_input.to_parquet(file_path)

    # Mock enrichment
    df_enriched = pd.DataFrame([
        {"domain": "a.com", "source": "abstract"}
    ])

    mock_enrich_chunk.return_value = (df_enriched, {"a.com"})

    # Run function
    enrich_company_parquet(input_dir, output_dir, seen_file)

    # Output file created
    output_files = list(output_dir.glob("*.parquet"))
    assert len(output_files) == 1

    # Output content is correct
    result_df = pd.read_parquet(output_files[0])
    assert not result_df.empty
    assert "domain" in result_df.columns

    # Seen domains saved
    assert seen_file.exists()
    seen_df = pd.read_csv(seen_file)
    assert "domain" in seen_df.columns

    # Function was called
    assert mock_enrich_chunk.called


# Test empty case
@patch("src.enrichment.enrich_company.enrich_company_chunk")
def test_enrich_company_parquet_empty(mock_enrich_chunk, tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    output_dir = tmp_path / "output"
    seen_file = tmp_path / "seen.csv"

    df_input = pd.DataFrame([{"domain": "a.com"}])
    (input_dir / "test.parquet").write_bytes(df_input.to_parquet())

    mock_enrich_chunk.return_value = (pd.DataFrame(), set())

    enrich_company_parquet(input_dir, output_dir, seen_file)

    assert not list(output_dir.glob("*.parquet"))
