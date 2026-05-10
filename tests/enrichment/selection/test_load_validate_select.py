import pandas as pd
import pytest
from unittest.mock import patch

from config.logger_config import logger

from src.enrichment.selection.preprocessing.load_validate_select import load_data, basic_filtering, select_candidates


@patch("src.enrichment.selection.preprocessing.load_validate_select.profile_dataset")
@patch("src.enrichment.selection.preprocessing.load_validate_select.load_csv_data")
@patch("src.enrichment.selection.preprocessing.load_validate_select.validate_file_path")
def test_load_data_success(
    mock_validate,
    mock_load_csv,
    mock_profile
):
    expected_df = pd.DataFrame({
        "company_name": ["Company A"]
    })

    mock_load_csv.return_value = expected_df

    result = load_data("test.csv")

    mock_validate.assert_called_once()
    mock_load_csv.assert_called_once_with("test.csv")
    mock_profile.assert_called_once_with(expected_df, logger)

    pd.testing.assert_frame_equal(result, expected_df)


def test_basic_filtering_raises_for_missing_columns():
    df = pd.DataFrame({
         "company_name": ["test"]
    })

    with pytest.raises(ValueError, match="Missing required columns"):
        basic_filtering(df)


def test_basic_filtering_removes_invalid_domains():
    df = pd.DataFrame({
        "domain": ["valid.com", "invalid_domain"],
        "company_name": ["Company A", "Company B"],
        "industry": ["Tech", "Tech"],
        "country": ["FI", "FI"]
    })

    result = basic_filtering(df)

    assert len(result) == 1
    assert result["domain"].iloc[0] == "valid.com"


def test_select_candidates_filters_by_min_score(sample_leads_df):
    result = select_candidates(
        sample_leads_df,
        min_score=90
    )
    print(result)
    print(result[["domain", "priority_score"]])
    assert len(result) == 1
    assert result["domain"].tolist() == ["a.com"]


def test_select_candidates_removes_duplicate_domains():
    df = pd.DataFrame({
        "domain": ["a.com", "a.com", "b.com"],
        "company_name": ["A1", "A2", "B"],
        "industry": ["Tech", "Tech", "Finance"],
        "country": ["FI", "FI", "SE"],
        "priority_score": [90, 80, 70],
        "is_valid_domain": [True, True, True],
        "size_category": ["medium", "medium", "medium"]
    })

    result = select_candidates(
        df,
        top_n=10,
        min_score=0
    )

    assert result["domain"].nunique() == 2


def test_select_candidates_respects_top_n(sample_leads_df):
    result = select_candidates(
        sample_leads_df,
        top_n=2,
        min_score=0
    )

    assert len(result) == 2

# def test_process_rows()
