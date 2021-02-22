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



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

