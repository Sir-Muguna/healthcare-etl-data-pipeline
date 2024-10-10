import os
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# Base URL format for the ICD files
BASE_URL = "https://www.cms.gov/files/document/valid-icd-{}-list.xlsx"

# Database connection details (update these if needed)
db_params = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'options': '-c search_path=private'
}

def check_version_exists(version):
    """Check if a specific version of the ICD list exists."""
    url = BASE_URL.format(version)
    response = requests.head(url)
    return response.status_code == 200, url

def get_latest_icd_version():
    """Determine the latest available ICD version by incrementing version numbers."""
    version = 10  # Start checking from ICD-10
    latest_version = None

    while True:
        exists, url = check_version_exists(version)
        if exists:
            latest_version = version
            print(f"ICD-{latest_version} is available at {url}")
            version += 1  # Check the next version
        else:
            if latest_version is not None:
                print(f"ICD-{version} not found. Latest available version is ICD-{latest_version}.")
                return latest_version, BASE_URL.format(latest_version)
            else:
                print("No valid ICD versions found.")
                return None, None

def insert_icd_data_to_db(df):
    """Insert data from the DataFrame into the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS icd_codes_raw (
            id SERIAL PRIMARY KEY,
            code VARCHAR(10),
            short_description TEXT,
            long_description TEXT,
            nf_excl TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        insert_query = """
        INSERT INTO icd_codes_raw (code, short_description, long_description, nf_excl)
        VALUES (%s, %s, %s, %s);
        """
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['CODE'],
                row['SHORT DESCRIPTION (VALID ICD-10 FY2024)'],
                row['LONG DESCRIPTION (VALID ICD-10 FY2024)'],
                row['NF EXCL']
            ))

        conn.commit()
        print("ICD data saved to database.")

    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def download_and_process_icd_file(icd_url):
    """Download the ICD file from the provided URL and insert data into the database."""
    try:
        response = requests.get(icd_url)
        if response.status_code == 200:
            # Read the Excel file into a DataFrame
            df = pd.read_excel(icd_url)

            # Debug: Print the first few rows of the DataFrame
            print("DataFrame contents:")
            print(df.head())

            # Insert the data into the database
            insert_icd_data_to_db(df)
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"Failed to process the Excel file: {e}")

if __name__ == "__main__":
    # Get the latest ICD version URL
    latest_version, latest_icd_url = get_latest_icd_version()

    # If a valid URL was found, download the ICD file and save metadata to the PostgreSQL database
    if latest_icd_url:
        download_and_process_icd_file(latest_icd_url)