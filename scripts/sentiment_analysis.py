import pandas as pd
from transformers import pipeline

df = pd.read_csv('data/reviews.csv')
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
df['sentiment_label'] = df['review'].apply(lambda x: sentiment_pipeline(x[:512])[0]['label'])  # Truncate for model
df['sentiment_score'] = df['review'].apply(lambda x: sentiment_pipeline(x[:512])[0]['score'])
df.to_csv('data/analyzed_reviews.csv', index=False)
print("Sentiment analysis complete.")