# Healthcare Data ETL Pipeline - Apache Airflow Project

## Project Overview

This project is a healthcare data ETL (Extract, Transform, Load) pipeline orchestrated using **Apache Airflow**. The pipeline extracts data from various healthcare-related sources, processes and transforms the data, and then loads it into Google BigQuery for further analysis and reporting. The project follows a modular structure, allowing each component (extraction, transformation, and load) to be run independently and easily maintained.

## Features

- **Data Extraction**: Extracts healthcare data from multiple APIs, including NPI, HCPCS, ICD Codes, and NDC Products.
- **Data Transformation**: Transforms and cleans the data using custom Python scripts, ensuring it's ready for analysis.
- **Data Load**: Loads the processed data into Google BigQuery.
- **Orchestration with Apache Airflow**: Automates the entire ETL process using Airflow DAGs with task dependencies.
- **Modular Structure**: Each stage of the ETL process is encapsulated in its own Python module, making it easy to maintain and modify.

## Technologies Used

- **Apache Airflow**: For orchestrating the ETL pipeline.
- **Python**: Core programming language used for the extraction, transformation, and load processes.
- **PostgreSQL**: Used for storing raw data before processing.
- **Google BigQuery**: Destination data warehouse for loading the processed data.
- **Google Cloud Platform (GCP)**: Used for cloud storage, data processing, and BigQuery services.
- **API Integration**: Pulls data from healthcare-related APIs such as the NPI Registry and FDA NDC.

## Project Directory Structure

```plaintext
├── dags/
│   ├── data_extraction/
│   │   ├── hcpcs_extraction.py
│   │   ├── icd_codes_extraction.py
│   │   ├── ndc_products_extraction.py
│   │   ├── npi_extraction.py
│   ├── data_transform/
│   │   ├── transformation.py
│   ├── data_load/
│   │   ├── bigquery_load.py
│   ├── health_data_pipeline.py  # Main Airflow DAG
```

### Key Python Files:
- **data_extraction/**: Contains scripts for extracting healthcare data from different sources.
  - `hcpcs_extraction.py`: Extracts HCPCS codes data.
  - `icd_codes_extraction.py`: Extracts ICD codes data.
  - `ndc_products_extraction.py`: Extracts NDC products data.
  - `npi_extraction.py`: Extracts NPI data from the registry.
  
- **data_transform/**: Contains the data transformation logic.
  - `transformation.py`: Processes and cleans the extracted data, preparing it for loading into BigQuery.

- **data_load/**: Contains the script for loading data into BigQuery.
  - `bigquery_load.py`: Loads the processed data into the respective BigQuery tables.

- **health_data_pipeline.py**: The Airflow DAG that orchestrates the entire ETL process.

## Setup Instructions

### Prerequisites

- **Apache Airflow**: Ensure that Airflow is installed and running.
- **Google Cloud SDK**: Install the Google Cloud SDK and authenticate using a service account with BigQuery and GCS permissions.
- **PostgreSQL**: Set up a PostgreSQL instance to store the raw extracted data.
- **Python Dependencies**: Install the required Python libraries by running:
  
  ```bash
  pip install -r requirements.txt
  ```

### Step 1: Google Cloud Setup

1. Create a Google Cloud project and enable BigQuery and Cloud Storage APIs.
2. Set up a **BigQuery dataset** where the processed data will be loaded.
3. Create a **Google Cloud Storage bucket** for storing intermediate or raw files (if needed).
4. Create and download a **Service Account JSON Key** for authentication.

### Step 2: Airflow Configuration

1. Copy the DAG files into the Airflow DAGs directory (`/opt/airflow/dags` or your custom path).
2. Update the database connection details in the extraction scripts (e.g., `hcpcs_extraction.py`) and in Airflow's connection settings.
3. Add the Google Cloud credentials to Airflow using the JSON key for authentication in BigQuery.

### Step 3: Run the Airflow DAG

1. Start the Airflow webserver and scheduler.
   
   ```bash
   airflow webserver
   airflow scheduler
   ```

2. In the Airflow UI, enable the DAG `health_data_pipeline`.

3. Monitor the DAG's execution through the Airflow UI. The tasks should run sequentially according to their defined dependencies.

### Step 4: Monitoring & Logs

- Airflow provides logs for each task, which can be viewed directly in the Airflow UI. Check logs for any potential issues during data extraction, transformation, or loading.

### DAG Overview

The main DAG `health_data_pipeline.py` orchestrates the ETL process:

1. **Data Extraction**:
    - `task_hcpcs_data_extraction`: Extracts HCPCS data.
    - `task_icd_data_extraction`: Extracts ICD data.
    - `task_fda_data_extraction`: Extracts FDA NDC product data.

2. **Data Transformation**:
    - `task_transformation`: Runs the data transformation using the `transformation.py` script.

3. **Data Load**:
    - `task_bigquery_load`: Loads the processed data into BigQuery using the `bigquery_load.py` script.

Each task is dependent on the successful completion of the previous task, ensuring a smooth ETL flow.

## Future Enhancements

- **Automated Data Validation**: Implement validation checks before loading data into BigQuery to ensure data quality.
- **Error Handling**: Add more comprehensive error handling and retry logic in Airflow tasks.
- **Monitoring and Alerts**: Integrate alerts (e.g., via Slack or email) to notify of task failures or data quality issues.
- **Scalability**: As data volume grows, consider using Dataproc with PySpark for more efficient data transformation.
