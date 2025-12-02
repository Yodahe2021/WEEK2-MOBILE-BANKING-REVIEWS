import psycopg2
import pandas as pd

conn = psycopg2.connect("dbname=bank_reviews user=postgres password=Happy host=localhost")
cur = conn.cursor()

# Insert banks
banks = [('CBE', 'Commercial Bank of Ethiopia Mobile'), ('BOA', 'Bank of Abyssinia Mobile'), ('Dashen', 'Dashen Bank Mobile')]
cur.executemany("INSERT INTO banks (bank_name, app_name) VALUES (%s, %s)", banks)

# Insert reviews
df = pd.read_csv('data/analyzed_reviews.csv')
bank_map = {'CBE':1, 'BOA':2, 'Dashen':3}
for _, row in df.iterrows():
    cur.execute("""
    INSERT INTO reviews (bank_id, review_text, rating, review_date, sentiment_label, sentiment_score, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (bank_map[row['bank']], row['review'], row['rating'], row['date'], row['sentiment_label'], row['sentiment_score'], row['source']))

conn.commit()
cur.close()
conn.close()
print("Data inserted.")