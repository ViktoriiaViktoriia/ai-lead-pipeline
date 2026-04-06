import pytest
from unittest.mock import patch

from src.utils.validators import (validate_folder_path, validate_file_path, validate_required_columns,
                                  validate_api_data, validate_run_mode)


# Test validate_folder_path()
def test_validate_folder_path_creates_folder(tmp_path):
    folder = tmp_path / "new_folder"

    validate_folder_path(folder)

    assert folder.exists()
    assert folder.is_dir()


def test_validate_folder_path_existing_folder(tmp_path):
    folder = tmp_path / "existing"
    folder.mkdir()

    validate_folder_path(folder)

    assert folder.exists()


def test_validate_folder_path_raises_if_not_folder(tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("data")

    with pytest.raises(ValueError):
        validate_folder_path(file)


# Test validate_file_path()
def test_validate_file_path_valid_csv(tmp_path):
    file = tmp_path / "data.csv"
    file.write_text("col1,col2\n1,2")

    validate_file_path(file)


def test_validate_file_path_not_exists(tmp_path):
    file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        validate_file_path(file)


def test_validate_file_path_not_a_file(tmp_path):
    folder = tmp_path / "folder"
    folder.mkdir()

    with pytest.raises(ValueError):
        validate_file_path(folder)


def test_validate_file_path_empty_file(tmp_path):
    file = tmp_path / "empty.csv"
    file.write_text("")

    validate_file_path(file)


def test_validate_file_path_wrong_extension(tmp_path):
    file = tmp_path / "data.txt"
    file.write_text("some data")

    validate_file_path(file)


# Test validate_required_columns()
def test_validate_required_columns_success(sample_df_large):
    validate_required_columns(sample_df_large, ["domain", "company_name"])


def test_validate_required_columns_missing(sample_df_small):
    with pytest.raises(ValueError):
        validate_required_columns(sample_df_small, ["domain", "company_name"])


# Test validate API data
def test_validate_api_data_success():
    api_data = {"name": "Test", "domain": "example.com"}

    assert validate_api_data(api_data, ["name", "domain"]) is True


def test_validate_api_data_missing_field():
    api_data = {"name": "Test"}

    assert validate_api_data(api_data, ["name", "domain"]) is False


# Test validate_run_mode()
@patch("src.utils.validators.RUN_MODE", "dry")
def test_validate_run_mode_valid_dry():
    validate_run_mode()


# Test invalid run mode
@patch("src.utils.validators.RUN_MODE", "invalid")
def test_validate_run_mode_invalid():
    with pytest.raises(ValueError):
        validate_run_mode()


# Test full mode with conformation
@patch("builtins.input", return_value="YES")
@patch("src.utils.validators.RUN_MODE", "full")
def test_validate_run_mode_full_confirm(mock_input):
    validate_run_mode()  # should pass


# Test full mode cancelled
@patch("builtins.input", return_value="NO")
@patch("src.utils.validators.RUN_MODE", "full")
def test_validate_run_mode_full_cancel(mock_input):
    with pytest.raises(RuntimeError):
        validate_run_mode()
