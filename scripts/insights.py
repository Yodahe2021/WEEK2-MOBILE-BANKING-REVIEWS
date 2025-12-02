import pandas as pd

df = pd.read_csv('data/analyzed_reviews.csv')

# Example insights
drivers = df[df['sentiment_label'] == 'POSITIVE'].groupby('bank')['theme'].value_counts().head(3)
pain_points = df[df['sentiment_label'] == 'NEGATIVE'].groupby('bank')['theme'].value_counts().head(3)

print("Drivers:", drivers)
print("Pain Points:", pain_points)

# Recommendations (manual)
recs = {
    'CBE': ['Optimize transfer speeds', 'Improve UI responsiveness'],
    'BOA': ['Fix login errors', 'Enhance customer support'],
    'Dashen': ['Add budgeting features', 'Reduce app crashes']
}
print("Recommendations:", recs)