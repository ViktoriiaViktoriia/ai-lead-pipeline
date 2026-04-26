# Common legal suffixes across countries
LEGAL_SUFFIXES = [
    "llc", "inc", "ltd", "gmbh", "oy", "oyj", "ab", "as",
    "bv", "sa", "sarl", "plc", "limited", "corporation", "corp"
]

# Basic country normalization mapping
COUNTRY_MAPPING = {
    "us": "US",
    "united states": "US",
    "usa": "US",
    "gb": "UK",
    "uk": "UK",
    "united kingdom": "UK",
    "de": "DE",
    "germany": "DE",
    "fr": "FR",
    "france": "FR",
    "fi": "FI",
    "finland": "FI",
    "Finland": "FI",
    "se": "SE",
    "sweden": "SE",
    "Sweden": "SE",
    "no": "NO",
    "Norway": "NO",
    "norway": "NO",
    "dk": "DK",
    "Denmark": "DK",
    "denmark": "DK",
    "is": "IS",
    "island": "IS",
    "Island": "IS"
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

# industry normalization
INDUSTRY_MAPPING = {
    "information technology": "technology",
    "it services": "technology",
    "software": "technology",
    "software development": "technology",
    "internet": "technology",
    "tech": "technology",

    "saas": "saas",

    "fintech": "fintech",

    "financial services": "finance",
    "banking": "finance",
    "insurance": "finance",

    "manufacturing": "manufacturing",

    "marketing and advertising": "marketing",
    "advertising services": "marketing",

    "education": "education",
    "education administration programs": "education",
}

ALLOWED_INDUSTRIES = set(INDUSTRY_MAPPING.values()) | {"unknown"}

NORDICS = ["FI", "SE", "NO", "DK", "IS"]

EU = [
    "FI", "SE", "NO", "DK", "IS",
    "DE", "NL", "FR", "ES", "IT",
    "PL", "BE", "AT", "IE"
]

SIZE_SCORE_MAP = {
    "enterprise": 4,
    "large": 3,
    "medium": 2,
    "small": 1
}

PRIORITY_WEIGHTS = {
    "geo": 3,
    "industry": 5,
    "size": 2,
    "employee_range": 2,
    "missing": 4
}

INDUSTRY_SCORE = {
    "fintech": 12,
    "saas": 12,
    "technology": 10,
    "manufacturing": 8,
    "finance": 10,
    "education": 8,
    "marketing": 8,
    "unknown": 5
}

REQUIRED_FIELDS = ["company_name", "industry", "country"]

RUN_MODE = "dry"  # "dry" | "mock" | "limited" | "full"

TEST_API_CALLS_LIMIT = 1
FULL_API_CALLS_LIMIT = 50

MAX_RETRIES = 1
BACKOFF_SEC = 2

TOP_LEADS_LIMIT = 100
CHECKPOINT_INTERVAL = 5
