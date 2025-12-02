import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
# Assuming db_config.py is in a folder accessible to the script
from db_config import DB_CONFIG 


# --- Configuration and Setup ---

# Define the path to your SQL schema file
SQL_SCHEMA_FILE = "scripts/create_table.sql"
CSV_FILE = "data/analyzed_reviews.csv"
BANK_NAME_COLUMN = 'bank' # Column name in your CSV that holds the bank abbreviation (e.g., 'CBE')


def connect():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG["port"]
    )


def create_tables(cur):
    """Executes the SQL schema file to create the necessary tables."""
    try:
        with open(SQL_SCHEMA_FILE, "r") as f:
            cur.execute(f.read())
        print(f"Tables successfully created using {SQL_SCHEMA_FILE}.")
    except FileNotFoundError:
        print(f"🚨 Error: SQL schema file '{SQL_SCHEMA_FILE}' not found. Cannot create tables.")
        raise


def insert_banks(cur):
    """Inserts bank names into the banks table, ensuring uniqueness."""
    banks = [
        ("CBE", "Commercial Bank of Ethiopia App"),
        ("BOA", "Bank of Abyssinia App"),
        ("Dashen", "Dashen Bank App")
    ]

    # Use executemany for efficiency with a small list of inserts
    cur.executemany(
        """
        INSERT INTO banks (bank_name, app_name)
        VALUES (%s, %s) 
        ON CONFLICT (bank_name) DO NOTHING;
        """,
        banks
    )
    print("Bank data inserted (or skipped if already existing).")


def get_bank_id_map(cur):
    """Fetches bank_name to bank_id mapping from Postgres."""
    cur.execute("SELECT bank_name, bank_id FROM banks;")
    return {name: id for name, id in cur.fetchall()}


def insert_reviews(cur, df, bank_id_map):
    """
    Optimized batch insertion of review data using execute_batch.
    """
    
    # 1. Map bank names to database IDs
    # Check if the bank name column exists
    if BANK_NAME_COLUMN not in df.columns:
        raise ValueError(f"Missing expected bank name column '{BANK_NAME_COLUMN}' in DataFrame.")
    
    df['bank_id'] = df[BANK_NAME_COLUMN].map(bank_id_map)
    
    # Check for unmapped banks or null IDs
    if df['bank_id'].isnull().any():
        unmapped = df[df['bank_id'].isnull()][BANK_NAME_COLUMN].unique()
        print(f"⚠️ Warning: {len(unmapped)} unique bank names not found in the 'banks' table: {unmapped}")
        # Drop rows that couldn't be mapped to prevent database errors
        df.dropna(subset=['bank_id'], inplace=True)
        print(f"Filtered DataFrame to {len(df)} rows after removing unmapped banks.")
    
    # Ensure bank_id is an integer (required by PostgreSQL foreign key)
    df['bank_id'] = df['bank_id'].astype(int)

    # 2. Prepare the data: create a list of tuples in the correct column order
    # (7 elements total matching the SQL insert query placeholders)
    data_to_insert = [
        (
            row["bank_id"], 
            row["review"],
            row["rating"],
            row["date"],
            row.get("sentiment_label"),
            row.get("sentiment_score"),
            row["source"]
        )
        for _, row in df.iterrows()
    ]
    
    # 3. Define the SQL template
    insert_query = """
        INSERT INTO reviews (
            bank_id, review_text, rating, review_date,
            sentiment_label, sentiment_score, source 
        ) VALUES (%s, %s, %s, %s, %s, %s, %s); 
    """
    
    # 4. Execute the batch insertion
    print(f"Executing batch insertion for {len(data_to_insert)} valid reviews...")
    execute_batch(cur, insert_query, data_to_insert)
    print("Batch insertion complete.")


def main():
    """Main execution function for connecting, creating tables, and inserting data."""
    try:
        # 1. Load data
        df = pd.read_csv(CSV_FILE)
        print(f"Loaded {len(df)} records from {CSV_FILE}.")

        # 2. Connect
        conn = connect()
        cur = conn.cursor()

        # 3. Create Tables
        create_tables(cur)

        # 4. Insert Banks
        insert_banks(cur)
        
        # 5. Get Bank Map
        bank_id_map = get_bank_id_map(cur) 

        # 6. Insert Reviews
        insert_reviews(cur, df, bank_id_map) 

        # 7. Commit and Close
        conn.commit()
        cur.close()
        conn.close()

        print("Data insertion process completed successfully! 🎉")

    except psycopg2.Error as e:
        print(f"\n❌ Database Operation Failed: {e}")
        # Note: If connection failed, conn might not exist
        if 'conn' in locals() and conn:
             conn.rollback() # Ensure transaction is rolled back on error
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()