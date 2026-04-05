import pytest
import pandas as pd
from unittest.mock import Mock, patch


@pytest.fixture
def sample_df_small():
    return pd.DataFrame([
        {"domain": "a.com"},
        {"domain": "b.com"}
    ])


@pytest.fixture
def sample_df_mid():
    return pd.DataFrame([
        {"domain": "a.com", "company_name": "Existing Co"},
        {"domain": "b.com"}
    ])


@pytest.fixture
def seen_domains():
    return set()


@pytest.fixture
def mock_abstract_client():
    client = Mock()
    client.enrich_company.return_value = {
        "company_name": "Test",
        "industry": "Tech",
        "country": "FI",
        "employee_range": "1-10"
    }
    client.REQUIRED_FIELDS = ["company_name", "industry", "country"]
    return client


@pytest.fixture
def mock_tech_client():
    client = Mock()
    client.enrich_company.return_value = {
        "company_name": "Test2",
        "industry": "Tech",
        "country": "FI"
    }
    client.REQUIRED_FIELDS = ["company_name", "industry", "country"]
    return client


@pytest.fixture
def mock_run_mode():
    def _set(mode):
        return patch("src.enrichment.run_mode.RUN_MODE", mode)
    return _set
