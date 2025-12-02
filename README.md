# Bank Reviews Analytics

## Overview
Analyzes Google Play Store reviews for CBE, BOA, and Dashen Bank to improve fintech apps.

## Methodology
- Task 1: Scrape 400+ reviews per bank using google-play-scraper; preprocess with pandas.
- Task 2: Sentiment via DistilBERT; themes via TF-IDF and LDA.
- Task 3: Store in PostgreSQL (banks and reviews tables).
- Task 4: Insights (drivers/pain points) and visualizations.

## Setup
1. Clone repo.
2. `pip install -r requirements.txt`.
3. Run scripts in `scripts/` order.
4. For DB: Install PostgreSQL, create `bank_reviews` DB.

## Branches
- task-1: Scraping/preprocessing.
- task-2: Analysis.
- task-3: DB.
- task-4: Insights.

## Reports
- Interim: reports/interim_report.pdf
- Final: reports/final_report.pdf