# Airflow - NYC Taxi Data Ingestion to GCP

This guide walks you through setting up Apache Airflow to ingest NYC taxi trip data from web URLs to Google Cloud Platform (GCS and BigQuery).

## Overview

The workflow performs the following steps:
1. **Download** taxi trip data (CSV.gz) from GitHub releases
2. **Upload** the CSV file to Google Cloud Storage (GCS)
3. **Create** BigQuery tables (partitioned main table, external table, temp table)
4. **Merge** data into the main partitioned table to avoid duplicates
5. **Cleanup** local files

Two DAGs are created:
- `gcp_green_taxi_ingestion` - Runs at 09:00 on the 1st of each month
- `gcp_yellow_taxi_ingestion` - Runs at 10:00 on the 1st of each month

## Prerequisites

- Docker and Docker Compose installed
- GCP account with:
  - BigQuery API enabled
  - Cloud Storage API enabled
  - Service account with appropriate permissions
- Service account JSON key file

## Setup Instructions

### 1. GCP Setup

#### Create a GCS Bucket
```bash
# Replace with your unique bucket name
export BUCKET_NAME="your-unique-bucket-name"
export PROJECT_ID="your-gcp-project-id"

gsutil mb -p $PROJECT_ID -l US gs://$BUCKET_NAME
```

#### Create a BigQuery Dataset
```bash
bq --project_id=$PROJECT_ID mk --location=US nyc_taxi_trips
```

#### Create Service Account and Download Key
```bash
# Create service account
gcloud iam service-accounts create airflow-sa \
    --description="Service account for Airflow" \
    --display-name="Airflow Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:airflow-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:airflow-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download key
gcloud iam service-accounts keys create service-account.json \
    --iam-account=airflow-sa@$PROJECT_ID.iam.gserviceaccount.com
```

### 2. Configure Environment

#### Copy and Edit Environment File
```bash
cd airflow
cp .env.example .env
```

Edit the `.env` file with your GCP details:
```bash
# Airflow Configuration
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# GCP Configuration
GCP_PROJECT_ID=your-gcp-project-id
GCP_LOCATION=US
GCP_BUCKET_NAME=your-globally-unique-bucket-name
GCP_DATASET=nyc_taxi_trips
```

#### Place Service Account Key
Move your service account JSON file to the `config` directory:
```bash
mv service-account.json config/
```

### 3. Start Airflow

#### Initialize Airflow (First Time Only)
```bash
# Set the Airflow UID (Linux/WSL only, skip on macOS)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Or use the default UID
echo "AIRFLOW_UID=50000" >> .env
```

#### Start the Services
```bash
docker compose up -d
```

This will start:
- **PostgreSQL** - Airflow metadata database
- **Airflow Webserver** - UI accessible at `http://localhost:8081`
- **Airflow Scheduler** - Handles DAG scheduling and execution

#### Check Service Status
```bash
docker compose ps
```

### 4. Install Additional Python Packages

The DAGs require Google Cloud provider packages. Install them in the running containers:

```bash
# Install in webserver
docker compose exec airflow-webserver pip install apache-airflow-providers-google==10.11.1

# Install in scheduler
docker compose exec airflow-scheduler pip install apache-airflow-providers-google==10.11.1
```

Alternatively, you can rebuild the image with requirements:
```bash
# Create a Dockerfile to extend the base image
cat > Dockerfile <<EOF
FROM apache/airflow:2.8.1-python3.11
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
EOF

# Build custom image
docker build -t airflow-gcp:latest .

# Update docker-compose.yaml to use the new image
# Change: image: apache/airflow:2.8.1-python3.11
# To: image: airflow-gcp:latest
```

### 5. Access Airflow UI

1. Open your browser and go to `http://localhost:8081`
2. Login with:
   - **Username**: `airflow`
   - **Password**: `airflow`
3. You should see two DAGs:
   - `gcp_green_taxi_ingestion`
   - `gcp_yellow_taxi_ingestion`

### 6. Run the DAGs

#### Option A: Manual Trigger
1. In the Airflow UI, click on the DAG name
2. Toggle the DAG to "On" (switch in the top left)
3. Click the "Play" button ▶️ to trigger a manual run
4. Select a specific execution date if needed

#### Option B: Backfill Multiple Dates
To process historical data from 2019-01-01 to 2021-08-01:

```bash
# Green taxi backfill
docker compose exec airflow-scheduler airflow dags backfill \
    --start-date 2019-01-01 \
    --end-date 2021-08-01 \
    gcp_green_taxi_ingestion

# Yellow taxi backfill
docker compose exec airflow-scheduler airflow dags backfill \
    --start-date 2019-01-01 \
    --end-date 2021-08-01 \
    gcp_yellow_taxi_ingestion
```

### 7. Monitor DAG Execution

#### View in Web UI
- **Graph View**: Visual representation of task dependencies
- **Grid View**: Historical runs and task statuses
- **Logs**: Click on any task to view detailed logs

#### Check Logs via CLI
```bash
# List DAG runs
docker compose exec airflow-scheduler airflow dags list-runs -d gcp_green_taxi_ingestion

# View task logs
docker compose exec airflow-scheduler airflow tasks logs gcp_green_taxi_ingestion download_taxi_data 2019-01-01
```

### 8. Verify Data in GCP

#### Check GCS
```bash
gsutil ls gs://$BUCKET_NAME/green/
gsutil ls gs://$BUCKET_NAME/yellow/
```

#### Check BigQuery
```bash
# List tables
bq ls $PROJECT_ID:nyc_taxi_trips

# Query data
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) as total_records FROM `'$PROJECT_ID'.nyc_taxi_trips.green_tripdata`'
```

Or in the BigQuery Console:
```sql
-- Check green taxi data
SELECT
    DATE(lpep_pickup_datetime) as pickup_date,
    COUNT(*) as trip_count
FROM `your-project-id.nyc_taxi_trips.green_tripdata`
GROUP BY pickup_date
ORDER BY pickup_date;

-- Check yellow taxi data
SELECT
    DATE(tpep_pickup_datetime) as pickup_date,
    COUNT(*) as trip_count
FROM `your-project-id.nyc_taxi_trips.yellow_tripdata`
GROUP BY pickup_date
ORDER BY pickup_date;
```

## DAG Configuration

### Schedule
- **Green Taxi**: `0 9 1 * *` (09:00 AM on the 1st of every month)
- **Yellow Taxi**: `0 10 1 * *` (10:00 AM on the 1st of every month)

### Catchup
Set to `True` - allows backfilling historical data

### Max Active Runs
Set to `3` - limits concurrent DAG runs

## Troubleshooting

### Issue: Permission Denied Errors
```bash
# Fix directory permissions
sudo chown -R $(id -u):0 ./dags ./logs ./plugins ./config
chmod -R 775 ./dags ./logs ./plugins ./config
```

### Issue: DAGs Not Showing Up
- Check that `.py` files are in the `dags/` directory
- Verify no syntax errors: `python dags/gcp_taxi_ingestion.py`
- Check scheduler logs: `docker compose logs airflow-scheduler`

### Issue: Google Cloud Authentication Failed
- Verify service account JSON is in `config/service-account.json`
- Check environment variables are set correctly in `.env`
- Ensure service account has required permissions

### Issue: Task Failures
1. Click on the failed task in the UI
2. View logs for error messages
3. Common issues:
   - Missing GCS bucket (create it first)
   - Missing BigQuery dataset (create it first)
   - Network issues (check internet connection)
   - Invalid file URLs (verify data exists for that month)

### Issue: Out of Memory
If processing large files, increase Docker memory:
- Docker Desktop → Settings → Resources → Memory (increase to 4-8GB)

## Stopping Airflow

```bash
# Stop services but keep data
docker compose stop

# Stop and remove containers (keeps volumes)
docker compose down

# Stop and remove everything including data
docker compose down -v
```

## Cleaning Up GCP Resources

```bash
# Delete BigQuery dataset
bq rm -r -f -d $PROJECT_ID:nyc_taxi_trips

# Delete GCS bucket
gsutil rm -r gs://$BUCKET_NAME

# Delete service account
gcloud iam service-accounts delete airflow-sa@$PROJECT_ID.iam.gserviceaccount.com
```

## Comparison: Airflow vs Kestra

| Feature | Airflow | Kestra |
|---------|---------|--------|
| Configuration | Python code (DAGs) | YAML files |
| Learning Curve | Steeper (requires Python knowledge) | Gentler (declarative YAML) |
| Flexibility | High (full Python programming) | Medium (plugin-based) |
| UI | Comprehensive, mature | Modern, user-friendly |
| Community | Large, established | Growing, newer |
| Best For | Complex workflows, custom logic | Quick setup, standard ETL |

## Next Steps

1. **Add Data Quality Checks**: Use Airflow's data quality operators
2. **Set Up Alerts**: Configure email or Slack notifications for failures
3. **Optimize Performance**: Partition handling, parallel processing
4. **Add More Sources**: Extend to other NYC datasets or data sources
5. **Implement Data Lineage**: Track data flow with tools like OpenLineage

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Google Cloud Provider](https://airflow.apache.org/docs/apache-airflow-providers-google/)
- [NYC TLC Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Data Source GitHub](https://github.com/DataTalksClub/nyc-tlc-data)

## Support

For issues or questions:
- Check Airflow logs: `docker compose logs -f`
- Review task logs in the Airflow UI
- Consult [Airflow Slack Community](https://apache-airflow-slack.herokuapp.com/)
