# AI-Powered Sales Lead Enrichment & Scoring Pipeline


## Overview

This project implements an AI-powered B2B lead (account) enrichment and scoring pipeline designed to simulate real-world Sales Operations workflows. The pipeline ingests company-level data (B2B accounts), enriches it using external APIs and AI models, and assigns a lead score to prioritize high-value business opportunities.

The system demonstrates how modern data pipelines integrate multiple data sources, handle incomplete or inconsistent data, and apply intelligent enrichment to improve decision-making in sales processes. This project focuses on account-level (company-level) lead scoring rather than individual contacts, following a B2B sales approach.


## Features

- Domain-based company enrichment pipeline
- API integration with rate-limit handling
- AI-powered fallback enrichment for missing data
- Data validation and cleaning
- Feature engineering and rule-based lead scoring
- End-to-end pipeline orchestration (Airflow)
- Integration with CRM systems (HubSpot)


## Tech Stack

- Python (data ingestion, transformation, pipeline logic)
- Apache Airflow (workflow orchestration & scheduling)
- External Enrichment API (company data enrichment)
- OpenAI API (AI-powered lead enrichment & scoring explanation)
- SQLAlchemy (ORM for database interaction)
- PostgreSQL (database: stores structured data)
- HubSpot API (CRM integration & lead management)
- Pytest (unit testing)
- GitHub Actions (CI/CD)
- Power BI (lead insights dashboard)


## Project Architecture

### System Architecture (general graph)

        Airflow (orchestrates everything)
                  ↓
      Ingestion → Enrichment → Scoring
                  ↓
            PostgreSQL (store results)
                  ↓
      Filter high-quality leads
                  ↓
            HubSpot (push leads)


### Data Flow Architecture (data-based graph)

               Raw data
                  ↓
               Cleaning
                  ↓
    Lead prioritization (EU/Scandinavia based companies)
                  ↓
    Abstract (Company Enrichment API)
                  ↓
    Technologychecker.io (Company data by domain API) 
                  ↓
       AI enrichment (fill gaps)
                  ↓
         Feature engineering
                  ↓
               Scoring
        

## Project Structure

| Directory / File                               | Description                                                               |
|:-----------------------------------------------|---------------------------------------------------------------------------|
| `ai_lead_pipeline/`                            | Root project directory                                                    |
| ├── `.github/`                                 | GitHub Actions for CI/CD                                                  |
| │   ├── `workflows/`                           | Contains CI/CD pipeline configurations                                    |
| │   │    └── `test.yml`                        | Runs unit tests and validates code on push/pull requests                  |
| ├── `config/`                                  | Stores configuration settings                                             |
| │   ├── `__init__.py`                          | Initialize the config package                                             | 
| │   ├── `config.py`                            | Stores environment variables (API keys, DB URL, constants)                |
| │   ├── `logger_config.py`                     | Logger configuration                                                      |
| │   └── `variables.py`                         | Stores variables                                                          |
| ├── `data/`                                    | Data storage layer for different pipeline stages                          |
| │   ├── `companies_raw_2023_q4.CSV`            | Raw input dataset (Base dataset source)                                   |
| │   ├── `processed/`                           | Stores intermediate pipeline outputs                                      |
| │   │    ├── `chunk_company_leads_2023_q4.CSV` | Chunked dataset with standardized schema                                  |
| │   │    ├── `cleaned_company_leads.CSV`       | Cleaned and standardized accounts data                                    | 
| │   │    ├── `enriched_company_leads.CSV`      | Company leads after AI/data enrichment                                    | 
| │   │    ├── `features.CSV`                    | Company leads (accounts) with engineered features for scoring             |
| │   │    └── `scored_company_leads.CSV`        | Final dataset with lead scores and explanations                           |
| ├── `logs/`                                    | Stores application logs                                                   |
| │   └── `app.log`                              | Main log file capturing pipeline execution details                        |
| ├── `src/`                                     | Core application logic (modular pipeline components)                      |
| │   ├── `ingestion/`                           | Handles data ingestion from CSV or external APIs                          |
| │   │   ├── `__init__.py`                      | Initialize the ingestion package                                          |
| │   │   ├── `load_company_leads.py`            | Loads raw leads data into the pipeline                                    |
| │   │   └── `metadata.py`                      | Creates unique id and timestamps for leads data                           |
| │   ├── `processing/`                          | Data processing layer (cleaning, normalization, and feature preparation)  |
| │   │   ├── `__init__.py`                      | Initialize the processing package                                         |
| │   │   ├── `cleaning/`                        | Modules for data cleaning and standardization                             |
| │   │   │    ├── `__init__.py`                 | Initializes the cleaning subpackage                                       |
| │   │   │    ├── `company_cleaner.py`          | Normalizes company names (case, punctuation, multilingual handling)       |
| │   │   │    ├── `clean_data.py`               | Cleaning orchestrator, cleans missing values, formats fields              |
| │   │   │    ├── `domain_cleaner.py`           | Normalizes and validates company domains (removes invalid/social domains) |
| │   │   │    ├── `industry_cleaner.py`         | Standardizes industry values (formatting and normalization)               |
| │   │   │    ├── `location_cleaner.py`         | Normalizes location data (country codes, city formatting)                 |
| │   │   │    ├── `size_cleaner.py`             | Standardizes company size into consistent ranges                          |
| │   │   │    └── `deduplicator.py`             | Removes duplicate company records                                         |
| │   │   ├── `data_quality/`                    | Dataset-level data quality checks and metrics                             |
| │   │   │    ├── `__init__.py`                 | Initializes the data_quality subpackage                                   |
| │   │   │    ├── `profiling.py`                | Dataset profiling overview (statistics)                                   |
| │   │   │    └── `checks.py`                   | Computes data quality metrics (missing domain rate, duplicates)           |
| │   │   └── `feature_engineering.py`           | Generates scores                                                          | 
| │   ├── `enrichment/`                          | Lead enrichment layer                                                     |
| │   │   ├── `__init__.py`                      | Initialize the enrichment package                                         |                                     
| │   │   ├── `ai_enrichment.py`                 | Uses OpenAI API to enrich leads                                           |
| │   │   └── `prompt_templates.py`              | Stores reusable prompt templates                                          |
| │   ├── `scoring/`                             | Lead scoring logic layer                                                  |
| │   │   ├── `__init__.py`                      | Initialize the scoring package                                            |                                     
| │   │   ├── `rule_based.py`                    | Implements rule-based scoring logic                                       |
| │   │   └── `scoring.py`                       | Combines features into final lead score and ranking                       |
| │   ├── `database/`                            | Database interaction and database schema definition                       |
| │   │   ├── `__init__.py`                      | Initialize the database package                                           |                                     
| │   │   ├── `models.py`                        | Defines database schema                                                   |
| │   │   └── `database_setup.py`                | Initializes DB connection and creates tables                              |
| │   ├── `integrations/`                        | Handles communication with external systems & APIs (business layer)       |
| │   │   ├── `__init__.py`                      | Initialize the integrations package                                       |
| │   │   ├── `hubspot_client.py`                | Sends and updates leads in HubSpot CRM via API                            |
| │   │   └── `base_client.py`                   | Reusable API request logic (authentication, retries, headers)             |
| │   ├── `pipeline/`                            | Pipeline orchestration layer                                              |
| │   │   └── `main.py`                          | Main script orchestrating tasks                                           |
| │   ├── `schemas/`                             | Defines the structure of datasets used across the data pipeline           |
| │   │   ├── `__init__.py`                      | Initialize the pipeline_schema package                                    |
| │   │   ├── `prepare_schema.py`                | Renames columns into standardized schema                                  |
| │   │   └──`leads_schema.py`                   | Schemas for downstream pipeline                                           |
| │   └── `utils/`                               | Utility functions layer                                                   |
| │   │   ├── `__init__.py`                      | Initialize the utils package                                              |                                     
| │   │   ├── `helpers.py`                       | Reusable utility functions used across the project                        |
| │   │   └── `validators.py`                    | Ensures data is correct before processing                                 |
| ├── `airflow/`                                 | Workflow orchestration using DAGs                                         |
| │   ├── `__init__.py`                          | Initialize the airflow package                                            |
| │   └── `lead_pipeline_dag.py`                 | Defines DAG for scheduling and automating pipeline tasks                  |
| ├── `tests/`                                   | Unit and integration tests                                                |
| │   ├── `__init__.py`                          | Initialize the tests package                                              |
| │   ├── `test_enrichment.py`                   | Tests AI enrichment logic and response handling                           |
| │   └── `test_scoring.py`                      | Tests scoring calculations                                                |
| ├── `dashboard/`                               | Data visualization layer                                                  |
| │   └── `lead_powerbi.pbix`                    | Dashboard built with Power BI showing top leads                           |
| ├── `.env`                                     | Stores API keys, database credentials (excluded from Git)                 |
| ├── `.gitignore`                               | Excludes unnecessary files                                                |
| ├── `LICENSE`                                  | License information                                                       |
| ├── `README.md`                                | Project documentation                                                     |
| └── `requirements.txt`                         | List of Python dependencies                                               |


## How to Get started

Install required dependencies:
pip install -r requirements.txt

## How to run the pipeline

1. Ingestion (run once). Loads raw company leads and prepares chunked data.
python3 -m src.pipeline.main --ingestion 

2. Cleaning (run once). Cleans and standardizes the dataset.
python3 -m src.pipeline.main --cleaning

3. Enrichment (run multiple times with different modes).
  3.1. Dry run (no API calls):
       python3 -m src.pipeline.main --enrichment --mode dry
  3.2. Mock run (simulated API):
       python3 -m src.pipeline.main --enrichment --mode mock
  3.3. Limited run (safe API testing):
       python3 -m src.pipeline.main --enrichment --mode limited
  3.4. Full run (production):
       python3 -m src.pipeline.main --enrichment --mode full

       Full mode will prompt for confirmation before running.

  Workflow explanation:
  - Run ingestion and cleaning once to prepare data.
  - Use mock or limited mode to test enrichment.
  - Run full mode only when ready for production.


## Dataset source

The project uses a publicly available company dataset (BigPicture dataset, 2023 Q4 version from Kaggle). Due to potential data quality issues (e.g., missing or invalid domains), preprocessing and validation steps were implemented to ensure compatibility with enrichment APIs.

Base dataset source: [Kaggle](https://www.kaggle.com/datasets/mfrye0/bigpicture-company-dataset?select=companies-2023-q4-sm.csv) which is made available
under the [ODC Attribution License](https://opendatacommons.org/licenses/by/1-0/index.html)

Company enrichment domain-based API: [Abstract](https://www.abstractapi.com/api/company-enrichment)

Company enrichment domain-based API: [Technologychecker.io](https://technologychecker.io/docs/api-reference/company-data/company-data-by-domain-api)

Priority-based enrichment:
Top 100 → Abstract (high quality)
Next → TechnologyChecker

## Tests

PYTHONPATH=. pytest -s tests/


## Example Output


## Contributions

Your feedback and contributions are welcome! Submit issues or pull requests to collaborate.


## License 

Licensed under the [Apache License 2.0](LICENSE)
