from src.enrichment.rule_based_enrichment import rule_based_enrich


def test_rule_based_enrich_basic():
    row = {"domain": "a.com", "company_name": "A"}

    result = rule_based_enrich(row)

    assert "buying_signal" in result
    assert "industry_confidence" in result


def test_rule_based_enrich_missing_fields():
    row = {}

    result = rule_based_enrich(row)

    assert isinstance(result, dict)


def test_rule_based_enrich_outputs_expected_keys():
    row = {
        "domain": "a.com",
        "company_name": "A"
    }

    result = rule_based_enrich(row)

    assert "industry_ai" in result
    assert "segment" in result
    assert "sales_relevance" in result
    assert "source" in result
