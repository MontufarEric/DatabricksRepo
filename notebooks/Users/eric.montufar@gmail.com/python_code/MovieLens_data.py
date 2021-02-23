# Databricks notebook source
# MAGIC %md
# MAGIC ## MovieLens Data analysis 
# MAGIC 
# MAGIC For this notebook, we will be using a MovieLens sample dataset. The data includes 100,000 ratings and 3,600 tag applications applied to 9,000 movies by 600 users and can be found in https://grouplens.org/datasets/movielens/latest/

# COMMAND ----------

# MAGIC %md
# MAGIC First, we import the libraries that we are going to use for the data visualization. Then we load the data we just downloades from the MovieLens website. The files were uploaded to an S3 bucket and loaded from there. 

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

# MAGIC %md 
# MAGIC 
# MAGIC As we cans ee in the table previews, the year of the movie is embeded in the movie title. In order to extract this information, I created a User Defined Function(UDF) that takes the year when available, otherwise it returns null. 

# COMMAND ----------

from pyspark.sql.functions import udf
import pyspark.sql.functions as F

def get_year(title):
  try:
    return(int(title[-5:-1]))
  except:
    return(None)


get_year_udf = udf(get_year)

# COMMAND ----------

movies = movies.withColumn("year", get_year_udf(movies.title))
movies.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As part of the analysis of this dataset, it would be useful to have the average rating for each movie. In the following cell of code, I aggregate over the ratings table to get the average rating for each movie ID. 

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.functions import bround
from pyspark.sql.functions import mean

ratings_agg = ratings.groupBy("movieId").agg(mean("rating").alias("avg_rating"))
ratings_agg = ratings_agg.withColumn("average_rating", ratings_agg.avg_rating.cast(FloatType())).drop("avg_rating").withColumnRenamed("average_rating", "avg_rating")
ratings_agg = ratings_agg.select("movieId",bround("avg_rating",2).alias("avg_rating"))
ratings_agg.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC Here we evaluate the average rating by year to identify if there is a trend in the ratings either to decrease or increase over the years. Visually, it is not possible to appreaciate such trend, but it was possible to identify some outlayer values in the year column. 
# MAGIC 
# MAGIC To achieve this, it was necessary to join the aggregated ratings table with movies table that includes the year as a column. 

# COMMAND ----------

joined_movies = movies.join(ratings_agg,"movieId")
joined_movies.select("year",'avg_rating').groupBy("year").mean().orderBy("year").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC As mentioned before, the year column contains some outlayers and null values. Thus, here I aggregate the data counting the number of movies by year. By doing this, we can see that there are some movies with years from the early 1900s. 

# COMMAND ----------

joined_movies.select("year",'avg_rating').groupBy("year").count().orderBy("year").display()

# COMMAND ----------

from pyspark.sql.functions import split,explode

exploded_movies = movies.withColumn("genres", explode(split("genres","[|]")))
exploded_movies.groupBy("genres").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Something worthy to analyze is to check wether the voters have a bias on a particular genre. This could be tested by taking the average rating by genre and see whether the rating distribution is uniform, that is, that there is no systematic bias towards a particular genre. 

# COMMAND ----------

rated_genres = exploded_movies.join(ratings,"movieId").select("genres","rating")
rated_genres.groupBy("genres").mean().display()


# COMMAND ----------

# MAGIC %md
# MAGIC By doing a visual analysis, it seems that the people rating the movies has no bias towards a particular genre. The distribution looks quite uniform, even though the sample is small. 

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now proceed to see which movies are the ones with the best score and those ones with the worst score. For this purpose, I will take the top 10 elements from the joined table that includes the movie title and the average rating. 

# COMMAND ----------

from pyspark.sql.functions import desc
joined_movies.select("title","avg_rating").orderBy(desc("avg_rating")).head(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Now I take the 10 movies with the lowest average rating to see the worst movies in our dataset. 

# COMMAND ----------

joined_movies.select("title","avg_rating").orderBy("avg_rating").head(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC Exportind the processed dataframes as parquet files to the S3 bucket we used to read the files. As a further step, I will be connecting this tables to AWS Redshift for their analysis. 

# COMMAND ----------

joined_movies.write.format("parquet").mode("Overwrite").option("path","s3a://filestoragedatabricks/MovieLensData/joined_df").save()

# COMMAND ----------


exploded_movies.write.format("parquet").mode("Overwrite").option("path","s3a://filestoragedatabricks/MovieLensData/exploded_df").save()

# COMMAND ----------

