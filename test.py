from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Import your extraction, transformation, and load functions
from data_extraction.hcpcs_extraction import process_hcpcs_data
from data_extraction.icd_codes_extraction import process_icd_data
from data_extraction.ndc_products_extraction import process_fda_data
from data_extraction.npi_extraction import create_npi_table, fetch_npi_data, insert_npi_data
from data_transform.transformation import process_data
from data_load.bigquery_load import process_bigquery_load

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'health_data_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for healthcare data',
    schedule_interval=timedelta(days=1),  # Set this as per your need
    catchup=False,
) as dag:

    # Task to extract hcpcs data
    extract_hcpcs_task = PythonOperator(
        task_id='extract_hcpcs',
        python_callable=process_hcpcs_data
    )

    # Task to extract icd codes
    extract_icd_task = PythonOperator(
        task_id='extract_icd_codes',
        python_callable=process_icd_data
    )

    # Task to extract ndc products
    extract_ndc_task = PythonOperator(
        task_id='extract_ndc_products',
        python_callable=process_fda_data
    )

    # Task to extract npi data
    extract_npi_task = PythonOperator(
        task_id='extract_npi_data',
        python_callable=npi_extraction
    )

    # Task to transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=process_data
    )

    # Task to load data into BigQuery
    load_to_bigquery_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=process_bigquery_load
    )

    # Set task dependencies
    [extract_hcpcs_task, extract_icd_task, extract_ndc_task, extract_npi_task] >> transform_task >> load_to_bigquery_task
