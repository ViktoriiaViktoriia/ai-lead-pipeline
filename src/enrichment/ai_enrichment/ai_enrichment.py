import json
from typing import Dict, Any, Optional

from config.logger_config import logger

from src.utils.validators import validate_ai_output


def build_prompt(row: dict) -> str:
    def safe(val):
        return str(val).replace("\n", " ").strip()

    return f"""
You are a data enrichment system for B2B lead scoring.

Analyze a company and return ONLY a valid structured JSON object.

Company: {safe(row.get('company_name'))}
Domain: {safe(row.get('domain'))}
Industry: {safe(row.get('industry'))}
Country: {safe(row.get('country'))}

Output format:
{{
  "industry_ai": "normalized industry category",
  "confidence": 0.0 to 1.0,
  "short_description": "one sentence description of what the company does",
  "segment": "high_fit | medium_fit | low_fit",
  "sales_relevance": 0.0 to 1.0,
  "buying_signal": 0 or 1,
}}

RULES:
- Do NOT include any explanation or extra text
- Output MUST be  valid JSON
- Do NOT include markdown
- If insure, lower confidence instead of guessing
- buying_signal = 1 only if strong commercial intent is likely
- sales_relevance reflects how suitable the company is for B2B outreach.
"""


def parse_response(content: Optional[str]) -> Dict[str, Any]:
    if not content:
        return {}

    try:
        parsed = json.loads(content)

        return {
            "industry_ai": parsed.get("industry_ai", "unknown"),
            "segment": parsed.get("segment", "unknown"),
            "confidence": float(parsed.get("confidence", 0.0)),
            "short_description": parsed.get("short_description", ""),
            "sales_relevance": float(parsed.get("sales_relevance", 0.0)),
            "buying_signal": int(parsed.get("buying_signal", 0)),
        }

    except (ValueError, TypeError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to parse AI response: {e}")
        return {}


def ai_enrich(row: Dict[str, Any], ai_client) -> Dict[str, Any]:
    """
    Row-level enrichment. Enrich a single row using AI client.
    """

    if not row.get("domain") and not row.get("company_name"):
        logger.info("Skipping AI enrichment: missing identifiers")
        return {}

    try:
        prompt = build_prompt(row)

        content = ai_client.generate(prompt)

        parsed = parse_response(content)
        if parsed:
            parsed["source"] = f"ai:{ai_client.model}"

        validated = validate_ai_output(parsed)

        return validated

    except Exception as e:
        logger.exception(f"Unexpected AI enrichment error: {e}")
        return {}
