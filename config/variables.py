# Common legal suffixes across countries
LEGAL_SUFFIXES = [
    "llc", "inc", "ltd", "gmbh", "oy", "oyj", "ab", "as",
    "bv", "sa", "sarl", "plc", "limited", "corporation", "corp"
]

# Basic country normalization mapping
COUNTRY_MAPPING = {
    "us": "united states",
    "usa": "united states",
    "gb": "united kingdom",
    "uk": "united kingdom",
    "de": "germany",
    "fr": "france",
    "fi": "finland",
}

# Standard mappings
SIZE_MAPPING = {
    "1-10": "small",
    "11-50": "small",
    "51-200": "medium",
    "201-500": "medium",
    "501-1000": "large",
    "1001-5000": "large",
    "5001-10000": "enterprise",
    "10000+": "enterprise",
}

INDUSTRY_MAPPING = {
    "information technology": "tech",
    "it services": "tech",
    "software": "tech",
    "software development": "tech",
    "internet": "tech",

    "financial services": "finance",
    "banking": "finance",
    "insurance": "finance",

    "marketing and advertising": "marketing",
    "advertising services": "marketing",

    "education": "education",
    "education administration programs": "education",
}

NORDICS = ["FI", "SE", "NO", "DK", "IS"]

EU = [
    "FI", "SE", "NO", "DK", "IS",
    "DE", "NL", "FR", "ES", "IT",
    "PL", "BE", "AT", "IE"
]

SIZE_SCORE_MAP = {
    "enterprise": 3,
    "large": 3,
    "medium": 2,
    "small": 1
}

PRIORITY_WEIGHTS = {
    "geo": 3,
    "size": 2,
    "missing": 4
}

RUN_MODE = "dry"  # "dry" | "mock" | "limited" | "full"

TEST_API_CALLS_LIMIT = 3
FULL_API_CALLS_LIMIT = 95

MAX_RETRIES = 2
BACKOFF_SEC = 2
