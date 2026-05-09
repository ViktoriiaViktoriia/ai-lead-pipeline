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


# test_basic_filtering_removes_invalid_domains

# test_select_candidates_filters_low_scores
# test_select_candidates_removes_duplicate_domains
# test_select_candidates_respects_top_n
# test_process_rows
