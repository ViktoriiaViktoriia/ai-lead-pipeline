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
