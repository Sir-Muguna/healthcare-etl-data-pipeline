import psycopg2
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import logging
import io
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

# Set the Google Cloud credentials environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/root/projects/portfolio_projects/data_engineering/healthcare-etl-data-pipeline/gcp/healthcare-etl-data-project-0499b67cb8ee.json"

# Database parameters
db_params = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

# BigQuery parameters
project_id = 'healthcare-etl-data-project'
dataset_id = 'health_data'

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Connect to PostgreSQL
def get_postgres_conn():
    return psycopg2.connect(
        host=db_params['host'],
        dbname=db_params['dbname'],
        user=db_params['user'],
        password=db_params['password'],
        port=db_params['port']
    )

# Load data from PostgreSQL
def fetch_data(query):
    conn = get_postgres_conn()
    try:
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return data, columns
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return [], []
    finally:
        cur.close()
        conn.close()

# Convert date/datetime objects to string
def convert_dates_to_string(data):
    return [
        [str(item) if isinstance(item, (str, bytes)) else item for item in row]
        for row in data
    ]

# Load data into BigQuery using load job without saving locally
def load_data_to_bigquery_with_load_job(table_name, data, columns):
    bucket_name = 'health_data_pipeline'  # Replace with your GCS bucket name
    
    # Convert data to CSV format in memory
    csv_buffer = io.StringIO()
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Reset buffer pointer to the start

    # Upload to Google Cloud Storage directly from memory
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{table_name}.csv')
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    # Load data from GCS to BigQuery
    uri = f'gs://{bucket_name}/{table_name}.csv'
    table_ref = client.dataset(dataset_id).table(table_name)

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(col, "STRING") for col in columns],  # Adjust as needed
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        max_bad_records=5  # Allow up to 5 bad rows
    )
    
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    logging.info(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_name}")

def load_tables_to_bigquery(tables):
    for table_name, query in tables.items():
        logging.info(f"Fetching data for table {table_name}...")
        data, columns = fetch_data(query)
        if data:
            logging.info(f"Loading data to BigQuery table {table_name}...")
            data = convert_dates_to_string(data)  # Convert dates to string
            load_data_to_bigquery_with_load_job(table_name, data, columns)
        else:
            logging.warning(f"No data found for table {table_name}.")

if __name__ == "__main__":
    tables = {
        "fda_ndc_transformed": "SELECT * FROM private.fda_ndc_transformed",
        "icd_codes_transformed": "SELECT * FROM private.icd_codes_transformed",
        "noc_codes_transformed": "SELECT * FROM private.noc_codes_transformed",
        "npi_registry_transformed": "SELECT * FROM private.npi_registry_transformed"
    }

    load_tables_to_bigquery(tables)
    logging.info("Data loading complete.")
