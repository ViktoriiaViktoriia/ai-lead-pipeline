from typing import Dict, Any

from config.logger_config import logger

from config.variables import NORDICS


def rule_based_enrich(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fallback enrichment for companies without AI processing.

    Generates segment, confidence score, short description, source (rule-based).
    """
    try:
        company_name = row.get("company_name")
        domain = row.get("domain")
        country = row.get("country")
        industry = row.get("industry")

        if not company_name and not domain:
            logger.info("Skipping enrichment", extra={"reason": "missing identifiers"})
            return {}

        # Segment logic
        segment, confidence = _assign_segment(country, industry)

        # Description generation
        description = _build_description(company_name, domain, industry, country)

        return {
            "industry_ai": industry,
            "segment": segment,
            "industry_confidence": confidence,
            "short_description": description,
            "source": "rule_based",
        }

    except ValueError as e:
        logger.warning(f"Validation error in rule-based enrichment: {e}")
        return {}

    except KeyError as e:
        logger.warning(f"Missing expected field: {e}")
        return {}

    except Exception as e:
        logger.exception(f"Unexpected error in rule-based enrichment: {e}")
        return {}


# Helper functions
def _assign_segment(country: str, industry: str) -> tuple[str, float]:
    """
    Assign segment and confidence using simple business rules.
    """
    try:
        # High priority: Nordics + tech
        if country in NORDICS and industry == "technology":
            return "high_fit", 0.75

        # Medium: either Nordics OR tech
        if country in NORDICS or industry == "technology":
            return "medium_fit", 0.6

        # Low: everything else
        return "low_fit", 0.4
    except Exception as e:
        logger.warning(f"Segment assignment failed: {e}")
        return "unknown", 0.0


def _build_description(
    company_name: str,
    domain: str,
    industry: str,
    country: str
) -> str:
    """
    Create simple human-readable description.
    """
    try:
        name = company_name if company_name else domain
        location = country or "an unspecified region"

        if industry == "unknown":
            return f"{name} operates in {location}."

        return f"{name} is a {industry} company based in {location}."

    except Exception as e:
        logger.warning(f"Description generation failed: {e}")
        return "Company description unavailable"
