import pytest
import pandas as pd
from unittest.mock import Mock, patch


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def fake_request(*args, **kwargs):
        raise RuntimeError("Real API call blocked during tests")

    # Block common request methods
    monkeypatch.setattr("requests.get", fake_request)
    monkeypatch.setattr("requests.post", fake_request)


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
def sample_df_large():
    df = pd.DataFrame([
        {
            "id": 0,
            "domain": "a.com",
            "company_name": "A Inc",
            "industry": "Tech",
            "country": "FI",
            "size_category": "small",
            "is_valid_domain": True,
        },
        {
            "id": 1,
            "domain": None,
            "company_name": "B Ltd",
            "industry": "IT",
            "country": "FI",
            "size_category": "medium",
            "is_valid_domain": False,
        },
        {
            "id": 2,
            "domain": "c.com",
            "company_name": "C Ltd",
            "industry": None,
            "country": None,
            "size_category": None,
            "is_valid_domain": True,
        },
        {
            "id": 3,
            "domain": "d.com",
            "company_name": "D Inc",
            "industry": "Tech",
            "country": "FI",
            "size_category": "unknown_value",
            "is_valid_domain": True,
        }
    ])

    df["is_valid_domain"] = df["domain"].notna()

    return df


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
