import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Database connection parameters
db_params = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

# Create a connection to PostgreSQL
def create_db_connection():
    conn_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    return create_engine(conn_string)

# Load a table from PostgreSQL into a Pandas DataFrame
def load_table(table_name, schema="private"):
    engine = create_db_connection()
    query = f"SELECT * FROM {schema}.{table_name}"
    return pd.read_sql(query, engine)

# Transform noc_codes DataFrame
def transform_noc_codes(noc_codes_df):
    noc_codes_df = noc_codes_df.rename(columns={
        "Unnamed: 0": "hcpcs_code",
        "NOC CODES - HCPCS 2024": "description",
        "Unnamed: 2": "add_date",
        "Unnamed: 3": "term_date"
    })
    noc_codes_df['add_date'] = pd.to_datetime(noc_codes_df['add_date'], format='%Y%m%d', errors='coerce')
    noc_codes_df = noc_codes_df[noc_codes_df['hcpcs_code'].notnull() & (noc_codes_df['hcpcs_code'] != 'HCPCS')]
    return noc_codes_df.drop(columns=["term_date"])

# Transform fda_ndc DataFrame
def transform_fda_ndc(fda_ndc_df):
    fda_ndc_df = fda_ndc_df.drop(columns=["finished", "listing_expiration_date"])
    fda_ndc_df['marketing_start_date'] = pd.to_datetime(fda_ndc_df['marketing_start_date'], format='%Y%m%d', errors='coerce')
    
    string_columns = [
        "labeler_name", "brand_name", "generic_name", "active_ingredient_name",
        "active_ingredient_strength", "package_description", "marketing_category",
        "dosage_form", "product_type", "route"
    ]
    
    for column in string_columns:
        if column in fda_ndc_df.columns:
            fda_ndc_df[column] = fda_ndc_df[column].str.strip().str.lower()
    
    return fda_ndc_df.drop_duplicates(subset=["package_ndc", "product_ndc"])

# Transform icd_codes DataFrame
def transform_icd_codes(icd_codes_df):
    icd_codes_df = icd_codes_df.drop(columns=["long_description", "nf_excl", "id"])
    icd_codes_df['description'] = icd_codes_df['short_description'].str.strip().str.lower()
    
    icd_codes_df['code'] = icd_codes_df['code'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
    icd_codes_df['description'] = icd_codes_df['description'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
    
    return icd_codes_df[['code', 'description']].dropna()

# Transform npi_registry DataFrame
def transform_npi_registry(npi_registry_df):
    npi_registry_df = npi_registry_df.drop(columns=["taxonomy_description", "address_purpose"])
    npi_registry_df['enumeration_date'] = pd.to_datetime(npi_registry_df['enumeration_date'], format='%Y%m%d', errors='coerce')
    
    name_columns = ['first_name', 'last_name', 'organization_name', 'address', 'city']
    for column in name_columns:
        npi_registry_df[column] = npi_registry_df[column].str.strip().str.title()
    
    return npi_registry_df.fillna('n/a')

# Write transformed DataFrame back to PostgreSQL
def write_table(df, table_name, schema="private"):
    engine = create_db_connection()
    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)

if __name__ == "__main__":
    # Load all tables into Pandas DataFrames
    fda_ndc_df = load_table("fda_ndc_raw", schema="private")
    icd_codes_df = load_table("icd_codes_raw", schema="private")
    noc_codes_df = load_table("noc_codes_raw", schema="private")
    npi_registry_df = load_table("npi_registry_raw", schema="private")

    # Transform the DataFrames
    noc_codes_df = transform_noc_codes(noc_codes_df)
    fda_ndc_df = transform_fda_ndc(fda_ndc_df)
    icd_codes_df = transform_icd_codes(icd_codes_df)
    npi_registry_df = transform_npi_registry(npi_registry_df)

    # Write transformed data to PostgreSQL
    write_table(fda_ndc_df, "fda_ndc_transformed")
    write_table(icd_codes_df, "icd_codes_transformed")
    write_table(noc_codes_df, "noc_codes_transformed")
    write_table(npi_registry_df, "npi_registry_transformed")
