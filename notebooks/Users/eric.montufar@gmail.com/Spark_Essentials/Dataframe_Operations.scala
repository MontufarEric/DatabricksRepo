// Databricks notebook source
// MAGIC %md
// MAGIC ## Dataframe Operations
// MAGIC 
// MAGIC Here we are going to implement some DataFrame aggregations and perform basic joins to illustrate the use of different DF operations. 

// COMMAND ----------

//Reading movies DF from our S3 location

val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("s3a://filestoragedatabricks/Spark-essentials-data/movies.json")

// COMMAND ----------

 // counting in two different ways
val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null

// with expr
moviesDF.selectExpr("count(Major_Genre)")


// COMMAND ----------

// counting all
moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls

// COMMAND ----------

// counting distinct
moviesDF.select(countDistinct(col("Major_Genre"))).show()

// COMMAND ----------

 // approximate count
moviesDF.select(approx_count_distinct(col("Major_Genre")))

// COMMAND ----------

// min and max
val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
moviesDF.selectExpr("min(IMDB_Rating)")

// COMMAND ----------

 // sum
moviesDF.select(sum(col("US_Gross")))
moviesDF.selectExpr("sum(US_Gross)")

// COMMAND ----------

  // avg
moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

// COMMAND ----------

// data science
moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Grouping

// COMMAND ----------

val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count()  // select count(*) from moviesDF group by Major_Genre

// COMMAND ----------

val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

// COMMAND ----------

val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))