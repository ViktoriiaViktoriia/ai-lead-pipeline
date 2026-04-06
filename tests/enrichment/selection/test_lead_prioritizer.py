import pandas as pd

from src.enrichment.selection.lead_prioritizer import (assign_geo_priority, compute_priority_score,
                                                       select_top_leads)
from config.variables import SIZE_SCORE_MAP


# Test assign_geo_priority()
def test_assign_geo_priority_nordics():
    assert assign_geo_priority("FI") == 3


def test_assign_geo_priority_eu():
    assert assign_geo_priority("DE") == 2


def test_assign_geo_priority_other():
    assert assign_geo_priority("US") == 1


def test_assign_geo_priority_none():
    assert assign_geo_priority(None) == 1


def test_assign_geo_priority_nan():
    assert assign_geo_priority(pd.NA) == 1


# Testing compute_priority_score()
def test_compute_priority_score_basic(sample_df_large):

    result = compute_priority_score(sample_df_large)

    assert "priority_score" in result.columns
    assert "geo_priority" in result.columns
    assert "size_score" in result.columns
    assert "data_missing_score" in result.columns

    assert result.iloc[0]["geo_priority"] == 3  # FI → Nordics
    assert result.iloc[0]["size_score"] >= 1


def test_compute_priority_score_missing_data(sample_df_large):
    result = compute_priority_score(sample_df_large)

    # 3 missing fields
    assert result.iloc[2]["data_missing_score"] == 3


def test_compute_priority_score_size_mapping(sample_df_large):
    result = compute_priority_score(sample_df_large)

    # Test size mapping works
    assert result.iloc[0]["size_score"] == SIZE_SCORE_MAP["small"]


def test_compute_priority_score_unknown_size(sample_df_large):
    result = compute_priority_score(sample_df_large)

    # Test unknown size_category fallback
    assert result.iloc[3]["size_score"] == 1


def test_compute_priority_score_types_are_numeric(sample_df_large):

    result = compute_priority_score(sample_df_large)

    assert result["geo_priority"].dtype in ["int64", "int32"]
    assert result["size_score"].dtype in ["int64", "int32"]
    assert result["priority_score"].dtype in ["int64", "float64"]


# Test select_top_leads
def test_select_top_leads_filters_invalid_rows(sample_df_large):

    print(sample_df_large["is_valid_domain"].unique())
    print(sample_df_large["is_valid_domain"].dtype)

    result = select_top_leads(sample_df_large)

    assert len(result) == 3
    assert result.iloc[0]["domain"] == "c.com"


def test_select_top_leads_respects_domain_validity(sample_df_large):

    # is_valid_domain test
    result = select_top_leads(sample_df_large)

    # Column exists
    assert "is_valid_domain" in result.columns

    # All returned rows must be valid
    assert result["is_valid_domain"].all()

    # Expected number of valid rows (before limit)
    assert len(result) == 3


def test_select_top_leads_limit(sample_df_large):

    # Test limit works
    result = select_top_leads(sample_df_large, limit=2)

    assert len(result) == 2


def test_select_top_leads_sorted_by_priority(sample_df_large):
    # Test sorted by priority
    result = select_top_leads(sample_df_large, limit=3)

    scores = result["priority_score"].tolist()
    print(f"Scores: {scores}")

    assert scores == sorted(scores, reverse=True)
