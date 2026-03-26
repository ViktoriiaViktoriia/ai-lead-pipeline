# AI-Powered Sales Lead Enrichment &amp; Scoring Pipeline


## Overview


## Features


## Tech Stack

- Python (data ingestion, transformation, pipeline logic)
- Apache Airflow (workflow orchestration & scheduling)
- OpenAI API (AI-powered lead enrichment & scoring explanation)
- SQLAlchemy (ORM for database interaction)
- PostgreSQL (database: stores structured data)
- HubSpot API (CRM integration & lead management)
- Pytest (unit testing)
- GitHub Actions (CI/CD)
- Power BI (lead insights dashboard)


## Project Architecture

        Airflow (orchestrates everything)
                  ↓
      Ingestion → Enrichment → Scoring
                  ↓
            PostgreSQL (store results)
                  ↓
      Filter high-quality leads
                  ↓
            HubSpot (push leads)


## Project Structure

| Directory / File                     | Description                                                         |
|--------------------------------------|---------------------------------------------------------------------|
| `ai_lead_pipeline/`                  | Root project directory                                              |
| ├── `.github/`                       | GitHub Actions for CI/CD                                            |
| │   ├── `workflows/`                 | Contains CI/CD pipeline configurations                              |
| │   │    └── `test.yml`              | Runs unit tests and validates code on push/pull requests            |
| ├── `config/`                        | Stores configuration settings                                       |
| │   ├── `__init__.py`                | Initialize the config package                                       | 
| │   ├── `config.py`                  | Stores environment variables (API keys, DB URL, constants)          |
| │   └── `logger_config.py`           | Logger configuration                                                |
| ├── `data/`                          | Data storage layer for different pipeline stages                    |
| │   ├── `__init__.py`                | Initialize the data package                                         |
| │   ├── `raw_leads.CSV`              | Raw input dataset                                                   |
| │   ├── `processed/`                 | Stores intermediate pipeline outputs                                |
| │   │    ├── `cleaned_leads.CSV`     | Cleaned and standardized leads data                                 | 
| │   │    ├── `enriched_leads.CSV`    | Leads after AI/data enrichment                                      | 
| │   │    ├── `features.CSV`          | Leads with engineered features for scoring                          |
| │   │    └── `scored_leads.CSV`      | Final dataset with lead scores and explanations                     |
| ├── `logs/`                          | Stores application logs                                             |
| │   └── `app.log`                    | Main log file capturing pipeline execution details                  |
| ├── `src/`                           | Core application logic (modular pipeline components)                |
| │   ├── `ingestion/`                 | Handles data ingestion from CSV or external APIs                    |
| │   │   ├── `__init__.py`            | Initialize the ingestion package                                    |
| │   │   └──`load_leads.py`           | Loads raw leads data into the pipeline                              |
| │   ├── `processing/`                | Data cleaning and feature preparation layer                         |
| │   │   ├── `__init__.py`            | Initialize the processing package                                   |                         
| │   │   ├── `clean_data.py`          | Cleans missing values, formats fields, removes duplicates           |
| │   │   └── `feature_engineering.py` | Generates scores                                                    | 
| │   ├── `enrichment/`                | Lead enrichment layer                                               |
| │   │   ├── `__init__.py`            | Initialize the enrichment package                                   |                                     
| │   │   ├── `ai_enrichment.py`       | Uses OpenAI API to enrich leads                                     |
| │   │   └── `prompt_templates.py`    | Stores reusable prompt templates                                    |
| │   ├── `scoring/`                   | Lead scoring logic layer                                            |
| │   │   ├── `__init__.py`            | Initialize the scoring package                                      |                                     
| │   │   ├── `rule_based.py`          | Implements rule-based scoring logic                                 |
| │   │   └── `scoring.py`             | Combines features into final lead score and ranking                 |
| │   ├── `database/`                  | Database interaction and schema definition                          |
| │   │   ├── `__init__.py`            | Initialize the database package                                     |                                     
| │   │   ├── `models.py`              | Defines database schema                                             |
| │   │   └── `database_setup.py`      | Initializes DB connection and creates tables                        |
| │   ├── `integrations/`              | Handles communication with external systems & APIs (business layer) |
| │   │   ├── `__init__.py`            | Initialize the integrations package                                 |
| │   │   ├── `hubspot_client.py`      | Sends and updates leads in HubSpot CRM via API                      |
| │   │   └── `base_client.py`         | Reusable API request logic (authentication, retries, headers)       |
| │   ├── `pipeline/`                  | Pipeline orchestration layer                                        |
| │   │   └── `main.py`                | Main script orchestrating tasks                                     |
| │   └── `utils/`                     | Utility functions layer                                             |
| │   │   ├── `__init__.py`            | Initialize the utils package                                        |                                     
| │   │   ├── `helpers.py`             | Reusable utility functions used across the project                  |
| │   │   └── `validators.py`          | Ensures data is correct before processing                           |
| ├── `airflow/`                       | Workflow orchestration using DAGs                                   |
| │   ├── `__init__.py`                | Initialize the airflow package                                      |
| │   └── `lead_pipeline_dag.py`       | Defines DAG for scheduling and automating pipeline tasks            |
| ├── `tests/`                         | Unit and integration tests                                          |
| │   ├── `__init__.py`                | Initialize the tests package                                        |
| │   ├── `test_enrichment.py`         | Tests AI enrichment logic and response handling                     |
| │   └── `test_scoring.py`            | Tests scoring calculations                                          |
| ├── `dashboard/`                     | Data visualization layer                                            |
| │   └── `lead_powerbi.pbix`          | Dashboard built with Power BI showing top leads                     |
| ├── `.env`                           | Stores API keys, database credentials (excluded from Git)           |
| ├── `.gitignore`                     | Excludes unnecessary files                                          |
| ├── `LICENSE`                        | License information                                                 |
| ├── `README.md`                      | Project documentation                                               |
| └── `requirements.txt`               | List of Python dependencies                                         |


## How to Get started


## Tests


## Contributions

Your feedback and contributions are welcome! Submit issues or pull requests to collaborate.


## License 

Licensed under the [Apache License 2.0](LICENSE)
