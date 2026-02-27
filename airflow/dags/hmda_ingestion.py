"""
HMDA Data Ingestion DAG
Downloads HMDA LAR data from CFPB Data Browser API (filtered by MSA),
converts to Parquet, uploads to GCS, loads into BigQuery via external table.

Using Parquet instead of CSV because:
- Parquet is self-describing (schema embedded in file), so BigQuery needs no
  schema definition and correctly handles nullable INT columns.
- CSV requires BigQuery to guess types via autodetect, which breaks on empty
  strings in columns like applicant_ethnicity-2 through -5 that use "" (not
  "NA") to signal a missing second/third ethnicity selection.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
GCP_DATASET = os.environ.get("GCP_DATASET", "lending_desert")
GCP_LOCATION = os.environ.get("GCP_LOCATION", "US")

HMDA_YEARS = [2022, 2023]
# Target MSAs: Boston, Atlanta, Birmingham AL, LA, Sacramento
TARGET_MSAS = "14460,12060,13820,31080,40900"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def create_hmda_dag(year):
    dag_id = f"hmda_ingestion_{year}"
    csv_name = f"hmda_{year}.csv"
    parquet_name = f"hmda_{year}.parquet"
    local_csv = f"/tmp/{csv_name}"
    local_parquet = f"/tmp/{parquet_name}"
    gcs_path = f"hmda/{parquet_name}"
    # Data Browser API requires: years + geography + at least one HMDA filter
    download_url = (
        f"https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
        f"?years={year}&msamds={TARGET_MSAS}&actions_taken=1,3&loan_purposes=1"
    )

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Ingest {year} HMDA LAR data to GCS and BigQuery",
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["hmda", "ingestion"],
    )

    with dag:
        download = BashOperator(
            task_id="download_hmda",
            bash_command=f'curl -sSL --retry 3 --retry-delay 10 -o {local_csv} "{download_url}" && head -1 {local_csv} | grep -q "activity_year" && ls -lh {local_csv}',
            execution_timeout=timedelta(minutes=30),
        )

        # Convert CSV to Parquet so BigQuery can infer a correct nullable schema
        # without autodetect guessing wrong types on empty-string null values.
        # na_values=['NA', ''] treats both the HMDA null marker and empty fields
        # as pandas NaN, which Parquet stores as proper nulls.
        convert_to_parquet = BashOperator(
            task_id="convert_to_parquet",
            bash_command=f"""python3 -c "
import pandas as pd
# dtype=str avoids mixed-type inference issues (e.g. loan_to_value_ratio
# has both numeric values and 'Exempt' for exempt lenders).
# replace() converts HMDA null markers and empty strings to None (Parquet null).
# Type casting is handled downstream in dbt staging models.
df = pd.read_csv('{local_csv}', dtype=str, keep_default_na=False)
df = df.replace({{'NA': None, '': None}})
df.to_parquet('{local_parquet}', index=False)
print(f'Converted {{len(df)}} rows, {{len(df.columns)}} columns to Parquet')
"
""",
        )

        upload_gcs = LocalFilesystemToGCSOperator(
            task_id="upload_to_gcs",
            src=local_parquet,
            dst=gcs_path,
            bucket=GCP_BUCKET_NAME,
        )

        # Parquet is self-describing — BigQuery reads the embedded schema directly,
        # no need to define column types manually (same pattern as the taxi dataset).
        create_ext_table = BigQueryInsertJobOperator(
            task_id="create_external_table",
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{GCP_DATASET}.hmda_lar_ext_{year}`
                        OPTIONS (
                            format = 'PARQUET',
                            uris = ['gs://{GCP_BUCKET_NAME}/{gcs_path}']
                        )
                    """,
                    "useLegacySql": False,
                }
            },
            location=GCP_LOCATION,
        )

        write_sql = (
            f"CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{GCP_DATASET}.hmda_lar_raw` AS"
            if year == HMDA_YEARS[0]
            else f"INSERT INTO `{GCP_PROJECT_ID}.{GCP_DATASET}.hmda_lar_raw`"
        )

        load_bq = BigQueryInsertJobOperator(
            task_id="load_to_bigquery",
            configuration={
                "query": {
                    "query": f"""
                        {write_sql}
                        SELECT * FROM `{GCP_PROJECT_ID}.{GCP_DATASET}.hmda_lar_ext_{year}`
                    """,
                    "useLegacySql": False,
                }
            },
            location=GCP_LOCATION,
        )

        drop_ext_table = BigQueryInsertJobOperator(
            task_id="drop_external_table",
            configuration={
                "query": {
                    "query": f"DROP TABLE IF EXISTS `{GCP_PROJECT_ID}.{GCP_DATASET}.hmda_lar_ext_{year}`",
                    "useLegacySql": False,
                }
            },
            location=GCP_LOCATION,
        )

        cleanup = BashOperator(
            task_id="cleanup",
            bash_command=f"rm -f {local_csv} {local_parquet}",
        )

        download >> convert_to_parquet >> upload_gcs >> create_ext_table >> load_bq >> drop_ext_table >> cleanup

    return dag


for y in HMDA_YEARS:
    globals()[f"hmda_ingestion_{y}"] = create_hmda_dag(y)
