import os
from datetime import datetime
from airflow.decorators import dag, task

# Add the path to include directory to the PYTHONPATH
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../include')))

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/projects/portfolio_projects/data_engineering/healthcare-etl-data-pipeline/include/gcp/healthcare-etl-data-project-0499b67cb8ee.json"


# Importing functions from your scripts
from data_extraction.hcpcs_extraction import extract_hcpcs_data
from data_extraction.icd_codes_extraction import download_and_process_icd_file
from data_extraction.ndc_products_extraction import fetch_fda_data
from data_extraction.npi_extraction import fetch_npi_data
from data_load.bigquery_load import write_table
from data_transform.transformation import transform_noc_codes, transform_fda_ndc, transform_icd_codes, transform_npi_registry

# Define your DAG
@dag(schedule_interval='@daily', start_date=datetime(2023, 1, 1), catchup=False)
def healthcare_etl_pipeline():
    
    # Extraction tasks
    @task
    def extract_hcpcs():
        return extract_hcpcs_data()  # Ensure this function returns the necessary DataFrame

    @task
    def extract_icd():
        return download_and_process_icd_file()  # Assuming this returns a DataFrame

    @task
    def extract_ndc():
        return fetch_fda_data()  # Assuming this function returns a DataFrame

    @task
    def extract_npi():
        return fetch_npi_data()  # Assuming this function returns a DataFrame

    # Transformation tasks
    @task
    def transform_noc_codes_task(noc_codes_df):
        return transform_noc_codes(noc_codes_df)

    @task
    def transform_fda_ndc_task(fda_ndc_df):
        return transform_fda_ndc(fda_ndc_df)

    @task
    def transform_icd_codes_task(icd_codes_df):
        return transform_icd_codes(icd_codes_df)

    @task
    def transform_npi_registry_task(npi_registry_df):
        return transform_npi_registry(npi_registry_df)

    # Loading tasks
    @task
    def load_noc_codes(transformed_df):
        write_table(transformed_df, "noc_codes_transformed")

    @task
    def load_fda_ndc(transformed_df):
        write_table(transformed_df, "fda_ndc_transformed")

    @task
    def load_icd_codes(transformed_df):
        write_table(transformed_df, "icd_codes_transformed")

    @task
    def load_npi_registry(transformed_df):
        write_table(transformed_df, "npi_registry_transformed")

    # Define task dependencies
    hcpcs_data = extract_hcpcs()
    icd_data = extract_icd()
    ndc_data = extract_ndc()
    npi_data = extract_npi()

    # Transforming data
    transformed_noc_codes = transform_noc_codes_task(hcpcs_data)
    transformed_fda_ndc = transform_fda_ndc_task(ndc_data)
    transformed_icd_codes = transform_icd_codes_task(icd_data)
    transformed_npi_registry = transform_npi_registry_task(npi_data)

    # Loading data
    load_noc_codes(transformed_noc_codes)
    load_fda_ndc(transformed_fda_ndc)
    load_icd_codes(transformed_icd_codes)
    load_npi_registry(transformed_npi_registry)

# Create the DAG instance
dag_instance = healthcare_etl_pipeline()
