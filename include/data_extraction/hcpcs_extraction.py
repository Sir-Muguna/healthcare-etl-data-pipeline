import requests
import zipfile
import os
from io import BytesIO
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection parameters
db_params = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432',
    'options': '-c search_path=private'
}

def get_quarter_and_abbr(month):
    """Determine the quarter and its corresponding abbreviation based on the month."""
    if month in [1, 2, 3]:
        return "january", "JAN"
    elif month in [4, 5, 6]:
        return "april", "APR"
    elif month in [7, 8, 9]:
        return "july", "JUL"
    else:
        return "october", "OCT"

def get_quarterly_zip_url(year=None, month=None):
    """Construct the quarterly ZIP URL based on the current year and quarter."""
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    quarter, _ = get_quarter_and_abbr(month)

    zip_url = f"https://www.cms.gov/files/zip/{quarter}-{year}-alpha-numeric-hcpcs-file.zip"
    return zip_url

def list_files_in_zip(zip_url):
    """Download the ZIP file and list all files inside."""
    try:
        response = requests.get(zip_url, stream=True)
        
        if response.status_code == 200:
            zip_file = BytesIO(response.content)

            with zipfile.ZipFile(zip_file, 'r') as z:
                print("Files in the ZIP archive:")
                for file in z.namelist():
                    print(file)
                return z.namelist()
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
            return []

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return []
    except zipfile.BadZipFile:
        print("The downloaded file is not a valid ZIP file.")
        return []

def download_and_save_to_postgres(zip_url, filename_prefix):
    """Download the ZIP file, extract the specific NOC file, and save the data to PostgreSQL."""
    try:
        response = requests.get(zip_url, stream=True)
        
        if response.status_code == 200:
            zip_file = BytesIO(response.content)

            with zipfile.ZipFile(zip_file, 'r') as z:
                file_to_extract = None
                for file in z.namelist():
                    if file.startswith(filename_prefix) and (file.endswith('.xls') or file.endswith('.xlsx')):
                        file_to_extract = file
                        break

                if file_to_extract:
                    print(f"Extracting '{file_to_extract}' and saving data to PostgreSQL...")

                    with z.open(file_to_extract) as extracted_file:
                        df = pd.read_excel(extracted_file)

                    engine = create_engine(
                        f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}?options={db_params["options"]}'
                    )

                    df.to_sql('noc_codes_raw', con=engine, if_exists='replace', index=False)

                    print(f"'{file_to_extract}' has been saved successfully to the PostgreSQL database.")

                else:
                    print(f"'{filename_prefix}' not found in the ZIP file with either .xls or .xlsx extension.")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
    except zipfile.BadZipFile:
        print("The downloaded file is not a valid ZIP file.")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_noc_codes_filename_prefix(year, quarter_abbr):
    """Construct the expected NOC codes file prefix with a space between the month and year."""
    return f"NOC codes_{quarter_abbr} {year}"

def extract_hcpcs_data():
    """Function to extract HCPCS data and save it to PostgreSQL."""
    now = datetime.now()
    year = now.year
    month = now.month

    quarter, quarter_abbr = get_quarter_and_abbr(month)
    zip_url = get_quarterly_zip_url(year, month)

    zip_file_list = list_files_in_zip(zip_url)
    noc_filename_prefix = get_noc_codes_filename_prefix(year, quarter_abbr)

    download_and_save_to_postgres(zip_url, noc_filename_prefix)

if __name__ == "__main__":
    extract_hcpcs_data()
