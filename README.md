# Lending Desert Discovery Engine

Identifies census tracts with high mortgage demand but high denial rates driven by appraisal bias — where a new lender can have the most impact.

## Architecture

```
CFPB (HMDA bulk CSV)  ──→  GCS  ──→  BigQuery (raw)  ──→  dbt (transform)  ──→  Streamlit
Census API (ACS)      ──→  GCS  ──→  BigQuery (raw)  ──↗
```

**Stack:** GCP, Terraform, Docker, Airflow, dbt, BigQuery, Streamlit

## Data Sources

| Source | Description | Granularity |
|--------|-------------|-------------|
| [HMDA](https://ffiec.cfpb.gov/) (2022-2023) | Mortgage application outcomes, denial reasons, applicant demographics | Per-application |
| [ACS 5-Year](https://www.census.gov/data/developers/data-sets/acs-5year.html) | Income, home values, race/ethnicity composition | Census tract |

**Target MSAs:** Boston, Atlanta, Birmingham AL, Los Angeles, Sacramento

## Setup

### 1. GCP Infrastructure

```bash
cd setup
terraform init && terraform apply
```

Creates: GCS bucket + BigQuery dataset (`lending_desert`).

### 2. Census API Key

Register at [api.census.gov](https://api.census.gov/data/key_signup.html).

### 3. Airflow

```bash
cd airflow
cp .env.example .env
# Edit .env: set GCP_PROJECT_ID, GCP_BUCKET_NAME, CENSUS_API_KEY
# Place service account JSON at airflow/config/service-account.json

docker compose build
docker compose up -d
```

UI: `http://localhost:8081` (airflow/airflow)

### 4. Run Pipeline

Trigger DAGs in order via Airflow UI:

1. `hmda_ingestion_2022` → `hmda_ingestion_2023` (downloads ~2GB each)
2. `acs_ingestion`
3. `dbt_transform`

### 5. Dashboard

For local development:
```bash
cd streamlit
pip install -r requirements.txt
streamlit run app.py
```

For Streamlit Cloud: connect repo, set `streamlit/app.py` as entrypoint, add GCP service account JSON as a secret under key `gcp_service_account`.

## dbt Models

| Model | Type | Description |
|-------|------|-------------|
| `stg_hmda` | view | Filters HMDA to 5 target MSAs, home purchase loans, originated/denied |
| `stg_acs` | view | Cleans ACS demographics, computes minority percentage |
| `mrt_tract_denials` | table | Per-tract denial rates, collateral denial share, denial reasons by race |
| `mrt_opportunity_score` | table | Joins denials + demographics, computes weighted opportunity score |

**Opportunity Score** = weighted rank of denial rate (30%), collateral denial share (30%), application volume (20%), minority percentage (20%). Higher = more opportunity.

## Dashboard

- **Tile 1:** Top opportunity tracts ranked by score (map when centroids added)
- **Tile 2:** Denial reason comparison — minority-majority vs white-majority tracts
