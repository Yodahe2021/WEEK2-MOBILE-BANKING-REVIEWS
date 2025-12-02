import psycopg2

# Connect to a default database (like 'postgres') instead of 'bank_reviews'
# The database you want to create (bank_reviews) must not be specified here.
# Change the dbname to 'postgres' or leave it out if your connection allows.
try:
    conn = psycopg2.connect("dbname=postgres user=postgres password=Happy host=localhost")
    conn.autocommit = True # Key: Set autocommit to TRUE to run non-transactional commands
    cur = conn.cursor()
    
    # Check if the database exists and create it if it doesn't
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'bank_reviews'")
    exists = cur.fetchone()
    
    if not exists:
        cur.execute("CREATE DATABASE bank_reviews")
        print("Database 'bank_reviews' created.")
    else:
        print("Database 'bank_reviews' already exists.")
        
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print(f"Error during database creation: {e}")