import requests
import psycopg2
from psycopg2 import sql

# Database connection details (update these if needed)
db_params = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'options': '-c search_path=private'
}

# Define the base URL of the API
api_url = "https://npiregistry.cms.hhs.gov/api/"

# Set parameters for API requests
params = {
    "number": "",
    "enumeration_type": "",
    "taxonomy_description": "",
    "name_purpose": "",
    "first_name": "",
    "use_first_name_alias": "",
    "last_name": "",
    "organization_name": "",
    "address_purpose": "",
    "city": "Topeka",
    "state": "KS",
    "postal_code": "",
    "country_code": "",
    "limit": 200,
    "skip": 0,
    "pretty": "on",
    "version": "2.1"
}

def create_npi_table():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS npi_registry_raw (
            number VARCHAR(50),
            enumeration_type VARCHAR(50),
            enumeration_date DATE,
            taxonomy_description VARCHAR(255),
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            organization_name VARCHAR(255),
            address VARCHAR(255),
            address_purpose VARCHAR(255),
            city VARCHAR(50),
            state VARCHAR(2),
            postal_code VARCHAR(20),
            country_code VARCHAR(2),
            telephone_number VARCHAR(20),
            fax_number VARCHAR(20),
            taxonomy_code VARCHAR(50),
            taxonomy_group VARCHAR(255),
            taxonomy_desc VARCHAR(255),
            license_no VARCHAR(50)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table npi_registry_raw created or already exists.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def fetch_npi_data(params):
    try:
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return []

def insert_npi_data(results):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO npi_registry_raw (
            number, enumeration_type, enumeration_date, taxonomy_description,
            first_name, last_name, organization_name, address, address_purpose,
            city, state, postal_code, country_code, telephone_number, fax_number,
            taxonomy_code, taxonomy_group, taxonomy_desc, license_no
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for result in results:
            row = (
                result.get("number", ""),
                result.get("enumeration_type", ""),
                result.get("basic", {}).get("enumeration_date", None),
                result.get("taxonomy_description", ""),
                result.get("basic", {}).get("first_name", ""),
                result.get("basic", {}).get("last_name", ""),
                result.get("basic", {}).get("organization_name", ""),
                result.get("addresses", [{}])[0].get("address_1", ""),
                result.get("basic", {}).get("address_purpose", ""),
                result.get("addresses", [{}])[0].get("city", ""),
                result.get("addresses", [{}])[0].get("state", ""),
                result.get("addresses", [{}])[0].get("postal_code", ""),
                result.get("addresses", [{}])[0].get("country_code", ""),
                result.get("addresses", [{}])[0].get("telephone_number", ""),
                result.get("addresses", [{}])[0].get("fax_number", ""),
                result.get("taxonomies", [{}])[0].get("code", ""),
                result.get("taxonomies", [{}])[0].get("taxonomy_group", ""),
                result.get("taxonomies", [{}])[0].get("desc", ""),
                result.get("taxonomies", [{}])[0].get("license", "")
            )
            cursor.execute(insert_query, row)

        conn.commit()
        print("Data successfully saved to the PostgreSQL database.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_npi_table()
    results = fetch_npi_data(params)
    if results:
        insert_npi_data(results)
    else:
        print("No results found.")