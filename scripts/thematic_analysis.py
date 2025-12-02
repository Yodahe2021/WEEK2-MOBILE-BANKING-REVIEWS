import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation

df = pd.read_csv('data/analyzed_reviews.csv')
vectorizer = TfidfVectorizer(stop_words='english', ngram_range=(1,2), max_features=1000)
tfidf_matrix = vectorizer.fit_transform(df['review'])

lda = LatentDirichletAllocation(n_components=5, random_state=42)
lda.fit(tfidf_matrix)

# Map topics to themes (manual example; adjust based on top words)
themes = {
    0: 'Account Access Issues',    # e.g., login, password
    1: 'Transaction Performance',  # e.g., slow transfer
    2: 'User Interface & Experience',  # e.g., UI, bugs
    3: 'Customer Support',     # e.g., help, response
    4: 'Feature Requests'      # e.g., new features
}

# --- FIX APPLIED HERE ---
# Convert the topic index array to a Pandas Series before mapping.
topic_indices = lda.transform(tfidf_matrix).argmax(axis=1)
df['theme'] = pd.Series(topic_indices).map(themes)
# --- END FIX ---

df.to_csv('data/analyzed_reviews.csv', index=False)
print("Thematic analysis complete.")