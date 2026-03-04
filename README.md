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
| [HMDA](https://ffiec.cfpb.gov/) (2022–2023) | Mortgage application outcomes, denial reasons, applicant demographics | Per-application |
| [ACS 5-Year](https://www.census.gov/data/developers/data-sets/acs-5year.html) | Income, home values, race/ethnicity composition | Census tract |

**Target MSAs:** Boston, Atlanta, Birmingham AL, Los Angeles, Sacramento

## Setup

### 1. Prerequisites

- GCP project with billing enabled
- Service account JSON with BigQuery and GCS permissions
- [Census API key](https://api.census.gov/data/key_signup.html)
- Docker + Docker Compose

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and set:

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_BUCKET_NAME` | Globally unique GCS bucket name |
| `GCP_DATASET` | BigQuery dataset name (default: `lending_desert`) |
| `GCP_CREDENTIALS_PATH` | Path to service account JSON |
| `GCP_LOCATION` | Multi-region location (default: `US`) |
| `CENSUS_API_KEY` | Your Census API key |

Place your service account JSON at the path specified by `GCP_CREDENTIALS_PATH`.

### 3. GCP Infrastructure

```bash
source .env
cd terraform
export TF_VAR_credentials=$GCP_CREDENTIALS_PATH
export TF_VAR_project=$GCP_PROJECT_ID
export TF_VAR_region=$GCP_REGION
export TF_VAR_location=$GCP_LOCATION
export TF_VAR_bq_dataset_name=$GCP_DATASET
export TF_VAR_gcs_bucket_name=$GCP_BUCKET_NAME
terraform init && terraform apply
```

Creates: GCS bucket + BigQuery dataset.

### 4. Airflow

```bash
cd airflow
docker compose build
docker compose up -d
```

UI: `http://localhost:8081` (airflow / airflow)

### 5. Run Pipeline

Trigger DAGs in order via the Airflow UI:

1. `hmda_ingestion_2022` → `hmda_ingestion_2023` (downloads ~2 GB each; CFPB downloads per-MSA to avoid row-count truncation, then concatenates and converts to Parquet)
2. `acs_ingestion` (fetches ACS 5-year tract data for MA, NH, GA, AL, CA)
3. `dbt_transform` (runs `dbt seed` then `dbt run`)

### 6. Dashboard

**Local:**
```bash
cd streamlit
pip install -r requirements.txt
streamlit run app.py
```

**Streamlit Cloud:** connect repo, set `streamlit/app.py` as entrypoint, add your GCP service account JSON as a secret under key `gcp_service_account`.

## dbt Models

| Model | Type | Description |
|-------|------|-------------|
| `stg_hmda` | view | Filters HMDA to 5 target MSAs, home purchase loans, originated/denied; adds `race_group` and collateral denial flag |
| `stg_acs` | view | Cleans ACS variable names, pads tract FIPS to 11 digits, computes minority percentage |
| `mrt_tract_denials` | table | Per-tract denial rates, collateral denial share, denial reasons by race; requires 10+ applications |
| `mrt_opportunity_score` | table | Joins denials + demographics, computes weighted opportunity score via percentile ranks |

**Opportunity Score** = weighted percentile rank across tracts in the same metro:

| Component | Weight |
|-----------|--------|
| Denial rate | 30% |
| Collateral denial share | 30% |
| Application volume (log-scaled) | 20% |
| Minority percentage | 20% |

Higher score = more lending opportunity.

**Seed:** `msa_lookup` — maps MSA/MD codes to metro names for the 5 target metros (7 records).

**Test:** `assert_all_msas_loaded` — verifies all MSA/MD codes from `msa_lookup` have at least one tract in `mrt_tract_denials`.

## Dashboard

- **Summary metrics:** tract count, average denial rate, average opportunity score per metro
- **Tile 1 — Opportunity Score Map:** interactive Mapbox map colored by opportunity score (red = higher), bubble size = application volume; top-50 tract table below
- **Tile 2 — Denial Reasons by Demographics:** grouped bar chart comparing collateral, DTI, credit history, employment, and cash denial rates between minority-majority and white-majority tracts
