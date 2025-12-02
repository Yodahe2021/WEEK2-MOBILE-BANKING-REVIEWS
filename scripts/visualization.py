import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from wordcloud import WordCloud

df = pd.read_csv('data/analyzed_reviews.csv')

# Sentiment by bank
sns.barplot(data=df, x='bank', y='sentiment_score')
plt.title('Average Sentiment Score by Bank')
plt.savefig('reports/sentiment_chart.png')

# Rating distribution
df['rating'].hist(by=df['bank'], figsize=(10,5))
plt.savefig('reports/rating_dist.png')

# Word cloud for CBE
text = ' '.join(df[df['bank']=='CBE']['review'])
wc = WordCloud().generate(text)
plt.figure()
plt.imshow(wc)
plt.axis('off')
plt.savefig('reports/wordcloud_cbe.png')

print("Visualizations saved.")