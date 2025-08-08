📌 Project Overview     
  🚀 ETL pipeline built on Databricks + PySpark + Delta Lake     
  📂 Data Source: [MovieLens](https://grouplens.org/datasets/movielens/) dataset (movies.csv, ratings.csv)     
  🛠 Pipeline Stages:         
      Bronze Layer: Raw data ingestion          
      Silver Layer: Data cleaning & transformation (extract year, clean titles, join datasets)          
      Gold Layer: Aggregated analytics-ready dataset      
  💾 Stored in Delta format for versioning, schema enforcement, and time travel     
  📊 Outputs: Top-rated movies, rating counts
