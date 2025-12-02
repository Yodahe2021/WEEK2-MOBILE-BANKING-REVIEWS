import pandas as pd

df = pd.read_csv('data/reviews.csv')
df.drop_duplicates(subset=['review', 'bank'], inplace=True)
df['date'] = pd.to_datetime(df['date']).dt.date  # Normalize
df.dropna(inplace=True)  # Handle missing
df.to_csv('data/reviews.csv', index=False)
print(f"Preprocessed: {len(df)} reviews.")