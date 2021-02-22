# Databricks notebook source
# MAGIC %pip install matplotlib
# MAGIC %pip install seaborn 

# COMMAND ----------

# MAGIC %md
# MAGIC ## MovieLens Data analysis 
# MAGIC 
# MAGIC For this notebook, we will be using a MovieLens sample dataset. The data includes 100,000 ratings and 3,600 tag applications applied to 9,000 movies by 600 users and can be found in https://grouplens.org/datasets/movielens/latest/

# COMMAND ----------

# MAGIC %md
# MAGIC First, we import the libraries that we are going to use for the data visualization. Then we load the data we just downloades from the MovieLens website. The files were uploaded to an S3 bucket and loaded from there. 

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

links = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("s3a://filestoragedatabricks/MovieLensData/links.csv")
movies = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("s3a://filestoragedatabricks/MovieLensData/movies.csv")
ratings = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("s3a://filestoragedatabricks/MovieLensData/ratings.csv")
tags = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("s3a://filestoragedatabricks/MovieLensData/tags.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC Once the data is loaded, we take a look at them by using the show action is spark. 

# COMMAND ----------

links.show()

# COMMAND ----------

movies.show()

# COMMAND ----------

ratings.show()

# COMMAND ----------

tags.show()

# COMMAND ----------

def return_age_bracket(age):
  if (age <= 12):
    return 'Under 12'
  elif (age >= 13 and age <= 19):
    return 'Between 13 and 19'
  elif (age > 19 and age < 65):
    return 'Between 19 and 65'
  elif (age >= 65):
    return 'Over 65'
  else: return 'N/A'

from pyspark.sql.functions import udf

maturity_udf = udf(return_age_bracket)
df = sqlContext.createDataFrame([{'name': 'Alice', 'age': 1}])
df.withColumn("maturity", maturity_udf(df.age))


# COMMAND ----------

from pyspark.sql.functions import udf
import pyspark.sql.functions as F

def get_year(title):
  return(title[-5:-1])


get_year_udf = udf(get_year)

# COMMAND ----------

movies = movies.withColumn("year", get_year_udf(movies.title))
movies.show()

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.functions import bround
from pyspark.sql.functions import mean

ratings_agg = ratings.groupBy("movieId").agg(mean("rating").alias("avg_rating"))
ratings_agg = ratings_agg.withColumn("average_rating", ratings_agg.avg_rating.cast(FloatType())).drop("avg_rating").withColumnRenamed("average_rating", "avg_rating")
ratings_agg = ratings_agg.select("movieId",bround("avg_rating",2).alias("avg_rating"))
ratings_agg.show()

# COMMAND ----------

joined_movies = movies.join(ratings_agg,"movieId")
joined_movies.show()


# COMMAND ----------

