import re

from config.logger_config import logger

from config.variables import SIZE_MAPPING


def normalize_employee_range(row: dict) -> str:
    """
    Normalize employee range into numeric format.
    This function takes a row, computes a cleaned value and returns a string, like "1000-2000".
    """

    employee_range_raw = row.get("employee_range") or row.get("size") or ""
    employee_range_raw = str(employee_range_raw).strip().lower()

    if not employee_range_raw:
        return "unknown"

    # Convert K notation → numbers
    employee_range_raw = employee_range_raw.replace("k", "000")

    # Normalize employee range: "1k-2k" → "1000-2000"
    match = re.match(r"(\d+)[-–](\d+)", employee_range_raw)
    if match:
        return f"{int(match.group(1))}-{int(match.group(2))}"

    # Match "1000+" → "1000+"
    match_plus = re.match(r"(\d+)\+", employee_range_raw)
    if match_plus:
        return f"{int(match_plus.group(1))}+"

    return "unknown"


def assign_size_category(row: dict) -> str:
    """
    Normalize employee range and assign size category.

    Returns both values.
    """

    employee_range = normalize_employee_range(row)

    existing_category = row.get("size_category")

    # Try exact mapping
    if employee_range in SIZE_MAPPING:
        return SIZE_MAPPING[employee_range]

    # Fallback to numeric logic
    if employee_range:
        try:
            if "+" in employee_range:
                lower = int(employee_range.replace("+", ""))
            else:
                lower = int(employee_range.split("-")[0])

            if lower <= 50:
                return "small"
            elif lower <= 500:
                return "medium"
            elif lower <= 5000:
                return "large"
            else:
                return "enterprise"

        except Exception as e:
            logger.exception(f"{e}")
            pass

    return existing_category or "unknown"
