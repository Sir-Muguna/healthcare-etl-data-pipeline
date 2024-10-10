import requests
import psycopg2

# PostgreSQL connection parameters
db_params = {
    'host': 'localhost',
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'options': '-c search_path=private'
}


BASE_API_URL = "https://api.fda.gov/drug/ndc.json"

def create_fda_table():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS fda_ndc_raw (
            id SERIAL PRIMARY KEY,
            product_ndc TEXT,
            generic_name TEXT,
            labeler_name TEXT,
            brand_name TEXT,
            active_ingredient_name TEXT,
            active_ingredient_strength TEXT,
            finished BOOLEAN,
            package_ndc TEXT,
            package_description TEXT,
            marketing_start_date TEXT,
            listing_expiration_date TEXT,
            manufacturer_name TEXT,
            marketing_category TEXT,
            dosage_form TEXT,
            product_type TEXT,
            route TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table fda_ndc_raw created or already exists.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def alter_table_columns():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        alter_query = """
        ALTER TABLE fda_ndc_raw 
        ALTER COLUMN product_ndc TYPE VARCHAR(512),
        ALTER COLUMN generic_name TYPE VARCHAR(512),
        ALTER COLUMN labeler_name TYPE VARCHAR(512),
        ALTER COLUMN brand_name TYPE VARCHAR(512),
        ALTER COLUMN active_ingredient_name TYPE VARCHAR(512),
        ALTER COLUMN active_ingredient_strength TYPE VARCHAR(512),
        ALTER COLUMN package_ndc TYPE VARCHAR(512),
        ALTER COLUMN package_description TYPE VARCHAR(1024),
        ALTER COLUMN marketing_category TYPE VARCHAR(512),
        ALTER COLUMN dosage_form TYPE VARCHAR(512),
        ALTER COLUMN product_type TYPE VARCHAR(512),
        ALTER COLUMN route TYPE VARCHAR(512),
        ALTER COLUMN manufacturer_name TYPE VARCHAR(512);
        """
        cursor.execute(alter_query)
        conn.commit()
        print("Table columns altered successfully.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def fetch_fda_data(base_url, query, limit=1000, max_records=10000):
    all_data = []
    skip = 0

    while True:
        url = f"{base_url}?search={query}&limit={limit}&skip={skip}"
        print(f"Fetching data from: {url}")
        
        try:
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                if "results" in data and data["results"]:
                    all_data.extend(data["results"])
                    print(f"Fetched {len(data['results'])} records, Total so far: {len(all_data)}")
                    
                    skip += limit
                    
                    if len(all_data) >= max_records:
                        all_data = all_data[:max_records]
                        break
                else:
                    print("No more data to fetch.")
                    break
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break
    
    return all_data

def insert_data_to_db(data):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO fda_ndc_raw (
            product_ndc, generic_name, labeler_name, brand_name, 
            active_ingredient_name, active_ingredient_strength, finished, 
            package_ndc, package_description, marketing_start_date, 
            listing_expiration_date, manufacturer_name, marketing_category, 
            dosage_form, product_type, route
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """

        for item in data:
            product_ndc = item.get('product_ndc', '')[:512]  # Adjusted length
            generic_name = item.get('generic_name', '')[:512]  # Adjusted length
            labeler_name = item.get('labeler_name', '')[:512]  # Adjusted length
            brand_name = item.get('brand_name', '')[:512]  # Adjusted length
            finished = item.get('finished', False)
            listing_expiration_date = item.get('listing_expiration_date', '')[:512]  # Adjusted length
            marketing_category = item.get('marketing_category', '')[:512]  # Adjusted length
            dosage_form = item.get('dosage_form', '')[:512]  # Adjusted length
            product_type = item.get('product_type', '')[:512]  # Adjusted length

            for active in item.get('active_ingredients', []):
                active_ingredient_name = active.get('name', '')[:512]  # Adjusted length
                active_ingredient_strength = active.get('strength', '')[:512]  # Adjusted length
                
                for pkg in item.get('packaging', []):
                    package_ndc = pkg.get('package_ndc', '')[:512]  # Adjusted length
                    package_description = pkg.get('description', '')[:1024]  # Adjusted length
                    marketing_start_date = pkg.get('marketing_start_date', '')[:512]  # Adjusted length
                    manufacturer_name = ','.join(item.get('openfda', {}).get('manufacturer_name', []))[:512]  # Adjusted length
                    route = ','.join(item.get('route', []))[:512]  # Adjusted length

                    try:
                        cursor.execute(insert_query, (
                            product_ndc, generic_name, labeler_name, brand_name, 
                            active_ingredient_name, active_ingredient_strength, finished, 
                            package_ndc, package_description, marketing_start_date, 
                            listing_expiration_date, manufacturer_name, marketing_category, 
                            dosage_form, product_type, route
                        ))
                    except psycopg2.DatabaseError as e:
                        print(f"Failed to insert data for {product_ndc}: {e}")

        conn.commit()
        print("Data inserted successfully into the database.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_fda_table()
    alter_table_columns()  # Alter the table columns to accommodate larger strings

    query = "finished:true"
    fda_data = fetch_fda_data(BASE_API_URL, query, limit=1000, max_records=10000)
    
    if fda_data:
        insert_data_to_db(fda_data)