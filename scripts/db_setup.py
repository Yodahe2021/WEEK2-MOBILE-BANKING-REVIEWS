import psycopg2

conn = psycopg2.connect("dbname=bank_reviews user=your_user password=your_pass host=localhost")
cur = conn.cursor()
with open('schema.sql', 'r') as f:
    cur.execute(f.read())
conn.commit()
cur.close()
conn.close()
print("DB setup complete.")