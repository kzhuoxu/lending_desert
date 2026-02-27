"""
ACS Data Ingestion DAG
Fetches American Community Survey 5-year estimates from Census API,
uploads to GCS, and loads into BigQuery.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os
import csv
import requests

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
GCP_DATASET = os.environ.get("GCP_DATASET", "lending_desert")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "US")
CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY")

# ACS 5-year estimates, tract-level
ACS_YEAR = 2022  # latest available 5-year ACS
ACS_VARIABLES = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B25077_001E": "median_home_value",
    "B02001_001E": "total_race",
    "B02001_002E": "white_alone",
    "B02001_003E": "black_alone",
    "B03003_001E": "total_hispanic_origin",
    "B03003_003E": "hispanic_latino",
}

# State FIPS for target MSAs
TARGET_STATES = {
    "25": "MA",  # Boston
    "33": "NH",  # Boston MSA spans MA + NH
    "13": "GA",  # Atlanta
    "01": "AL",  # Birmingham
    "06": "CA",  # LA, Sacramento
}

LOCAL_CSV = "/tmp/acs_tract_data.csv"
GCS_PATH = "acs/acs_tract_data.csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def fetch_acs_data():
    """Fetch ACS data from Census API for all target states and write to CSV."""
    var_codes = ",".join(ACS_VARIABLES.keys())
    all_rows = []

    for state_fips, state_abbr in TARGET_STATES.items():
        url = (
            f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
            f"?get=NAME,{var_codes}"
            f"&for=tract:*&in=state:{state_fips}"
            f"&key={CENSUS_API_KEY}"
        )
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        header = data[0]
        for row in data[1:]:
            record = dict(zip(header, row))
            # Build 11-digit census tract FIPS
            record["census_tract"] = record["state"] + record["county"] + record["tract"]
            record["state_abbr"] = state_abbr
            all_rows.append(record)

    # Write CSV
    fieldnames = (
        ["census_tract", "state_abbr", "NAME", "state", "county", "tract"]
        + list(ACS_VARIABLES.keys())
    )
    with open(LOCAL_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k) for k in fieldnames})

    print(f"Wrote {len(all_rows)} tracts to {LOCAL_CSV}")


dag = DAG(
    "acs_ingestion",
    default_args=default_args,
    description="Ingest ACS 5-year tract data to GCS and BigQuery",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["acs", "ingestion"],
)

with dag:
    fetch = PythonOperator(
        task_id="fetch_acs_data",
        python_callable=fetch_acs_data,
    )

    upload_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=LOCAL_CSV,
        dst=GCS_PATH,
        bucket=GCP_BUCKET_NAME,
    )

    load_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCP_BUCKET_NAME}/{GCS_PATH}"],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": GCP_DATASET,
                    "tableId": "acs_tract_raw",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location=GCP_LOCATION,
    )

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -f {LOCAL_CSV}",
    )

    fetch >> upload_gcs >> load_bq >> cleanup
