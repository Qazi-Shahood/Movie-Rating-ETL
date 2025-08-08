# Databricks notebook source
# MAGIC %md
# MAGIC Bronze Layer

# COMMAND ----------

from pyspark.sql import SparkSession, functions as Fs
from pyspark.sql.functions import to_date, col, udf, from_unixtime, trim, regexp_replace, split, regexp_extract

spark = SparkSession.builder.appName("movie_rating").getOrCreate()

movie_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/movie_rating/raw/movies.csv") \
    .limit(500)

rating_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/movie_rating/raw/ratings.csv") \
    .limit(500)

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer

# COMMAND ----------

# drop nulls
movie_df = movie_df.na.drop()
rating_df = rating_df.na.drop()

# drop 0 ratings
rating_df = (
    rating_df
    .filter(rating_df.rating > 0)
    .withColumn("rating_date", from_unixtime(col('timestamp')).cast("date"))
    .drop("timestamp")
)
movie_df = (
    movie_df
    .withColumn("title_trim", trim(col('title')))
    .withColumn("year", regexp_extract(col('title_trim'), r"\((\d{4})\)$", 1).cast('int'))
    .withColumn("title_clean", regexp_extract(col('title_trim'), r"^(.*?)(?:\s\(\d{4}\))?$", 1))
    .withColumn('genres_array', split(col('genres'), r"\|"))
    .drop('title', 'title_trim')
    .withColumnRenamed('title_clean', 'title')
)


# COMMAND ----------

# Save in Bronze layer 
rating_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("/Volumes/workspace/movie_rating/bronze/ratings_cleaned")
movie_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("/Volumes/workspace/movie_rating/bronze/movies_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze Layer

# COMMAND ----------

rating_df_cleaned = spark.read.format("delta").load("/Volumes/workspace/movie_rating/bronze/ratings_cleaned")
movie_df_cleaned = spark.read.format("delta").load("/Volumes/workspace/movie_rating/bronze/movies_cleaned")
rating_df_cleaned.show(5)
movie_df_cleaned.show(5)

# COMMAND ----------

merged_df = movie_df_cleaned.join(rating_df_cleaned, on='movieId', how='inner')
merged_df.show(5)

# COMMAND ----------


aggregated_df = merged_df.groupBy('movieId').agg(
    Fs.avg('rating').alias('avg_rating'),
    Fs.count('rating').alias('total_rating')
    )

final_df = aggregated_df.join(merged_df, on='movieId', how='inner')
final_df = final_df.orderBy('total_rating', ascending=False)

final_df = final_df.filter(final_df.total_rating < 50)

final_df.select('movieId', 'title', 'year', 'avg_rating', 'total_rating').show(5)

final_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save('/Volumes/workspace/movie_rating/gold/movie_ratings_delta')

# COMMAND ----------

# MAGIC %md
# MAGIC Quality Checks

# COMMAND ----------

print("Bronze movies count: ", movie_df.count())
print("Silver movies count: ", movie_df_cleaned.count())

# COMMAND ----------

rating_df_cleaned.filter(rating_df_cleaned.rating.isNull()).count()