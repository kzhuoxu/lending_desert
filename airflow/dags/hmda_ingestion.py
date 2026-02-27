"""
HMDA Data Ingestion DAG
Downloads HMDA LAR data from CFPB Data Browser API (one MSA at a time),
converts to Parquet, uploads to GCS, loads into BigQuery via external table.

Downloading per-MSA rather than all MSAs in one request because the CFPB
Data Browser API silently truncates large responses — LA and Boston each
exceed ~100k rows and were missing from the combined-MSA download.

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
# Must mirror msa_lookup seed — one code per target MSA
# Metropolitan Division (MD) codes from FFIEC HMDA data.
# Boston (14460 parent) → MDs 14454 (MA counties) + 40484 (NH counties)
# LA (31080 parent) → MDs 31084 (LA-Long Beach-Glendale) + 11244 (Anaheim-Santa Ana-Irvine)
# Atlanta, Birmingham, Sacramento have no MD split — use MSA code directly.
TARGET_MSA_CODES = [14454, 40484, 12060, 13820, 31084, 11244, 40900]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def create_hmda_dag(year):
    dag_id = f"hmda_ingestion_{year}"
    parquet_name = f"hmda_{year}.parquet"
    local_parquet = f"/tmp/{parquet_name}"
    gcs_path = f"hmda/{parquet_name}"

    # Per-MSA CSV paths resolved at DAG-creation time so the bash and Python
    # subprocesses receive plain strings with no runtime variable lookups.
    msa_csv_paths = [f"/tmp/hmda_{year}_{msa}.csv" for msa in TARGET_MSA_CODES]
    msa_csv_paths_repr = repr(msa_csv_paths)
    all_tmp_files = " ".join(msa_csv_paths)
    msa_codes_str = " ".join(str(m) for m in TARGET_MSA_CODES)

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
        # Download each MSA individually to avoid CFPB API row-count truncation.
        # Validate the CSV header after each download so a silent API error page
        # (which would still exit 0 from curl) is caught immediately.
        download = BashOperator(
            task_id="download_hmda",
            bash_command=f"""
set -e
for msa in {msa_codes_str}; do
    echo "Downloading MSA $msa for {year}..."
    curl -sSL --retry 3 --retry-delay 10 \\
        -o /tmp/hmda_{year}_$msa.csv \\
        "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?years={year}&msamds=$msa&actions_taken=1,3&loan_purposes=1"
    head -1 /tmp/hmda_{year}_$msa.csv | grep -q "activity_year" || (echo "ERROR: bad response for MSA $msa" && exit 1)
    echo "MSA $msa: $(wc -l < /tmp/hmda_{year}_$msa.csv) rows"
done
""",
            execution_timeout=timedelta(minutes=60),
        )

        # Read each per-MSA CSV, concatenate, replace HMDA null markers, write Parquet.
        # dtype=str avoids mixed-type inference (e.g. loan_to_value_ratio has both
        # numeric strings and 'Exempt'). Casting is handled in dbt staging models.
        # Explicit pyarrow string schema ensures columns that are entirely null in one
        # year (e.g. applicant_ethnicity_5) aren't serialised as Arrow null/INT64,
        # which would conflict with the STRING schema established by the first year's
        # CREATE OR REPLACE TABLE when the second year runs INSERT INTO.
        convert_to_parquet = BashOperator(
            task_id="convert_to_parquet",
            bash_command=f"""python3 -c "
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
files = {msa_csv_paths_repr}
dfs = [pd.read_csv(f, dtype=str, keep_default_na=False) for f in files]
combined = pd.concat(dfs, ignore_index=True).replace({{'NA': None, '': None}})
schema = pa.schema([(col, pa.string()) for col in combined.columns])
pq.write_table(pa.Table.from_pandas(combined, schema=schema), '{local_parquet}')
print(f'Combined {{len(combined)}} rows from {{len(dfs)}} MSA files: {{[len(d) for d in dfs]}}')
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
        # no need to define column types manually.
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
            bash_command=f"rm -f {local_parquet} {all_tmp_files}",
        )

        download >> convert_to_parquet >> upload_gcs >> create_ext_table >> load_bq >> drop_ext_table >> cleanup

    return dag


for y in HMDA_YEARS:
    globals()[f"hmda_ingestion_{y}"] = create_hmda_dag(y)
