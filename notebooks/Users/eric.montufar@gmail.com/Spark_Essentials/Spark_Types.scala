// Databricks notebook source
// MAGIC %md 
// MAGIC ## Common Data Types
// MAGIC 
// MAGIC Here we are going to add different data types to our data frames.

// COMMAND ----------

// reading dataframes

val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


// COMMAND ----------

// adding a plain value to a DF (lit works for any type of value)

moviesDF.select(col("Title"), lit(47).as("plain_value"))

// COMMAND ----------

// Booleans
// equalTo same as ===

val dramaFilter = col("Major_Genre") equalTo "Drama"
val goodRatingFilter = col("IMDB_Rating") > 7.0
val preferredFilter = dramaFilter and goodRatingFilter



// COMMAND ----------

// applying the filter 
moviesDF.select("Title").where(dramaFilter)

// COMMAND ----------

 // + multiple ways of filtering and creating ann additional column to the DF

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

// COMMAND ----------

// filter on a boolean column that we just created 

moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")


// COMMAND ----------

 // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

// COMMAND ----------

// Numbers
// math operators
val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

// COMMAND ----------

// correlation = number between -1 and 1
println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)


// COMMAND ----------

// Strings

val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

// COMMAND ----------

// capitalization: initcap, lower, upper
carsDF.select(initcap(col("Name")))

// COMMAND ----------

// contains
carsDF.select("*").where(col("Name").contains("volkswagen"))

// COMMAND ----------

// regex
// filtering based on the content of a string col

val regexString = "volkswagen|vw"
val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

// COMMAND ----------

// Replace text using regex

vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC ## Complex Data Types

// COMMAND ----------

// Reading our DF  

val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

// COMMAND ----------

  // Dates
val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) 
// conversion

// COMMAND ----------

moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

// COMMAND ----------

moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Dealing with Nulls

// COMMAND ----------

// select the first non-null value
moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

// COMMAND ----------

// checking for nulls
moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

// COMMAND ----------

// nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

// COMMAND ----------

// removing nulls
moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

// COMMAND ----------

// replace nulls
moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

// COMMAND ----------

// complex operations ifnull  nvl2 to replace nulls 
moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Excercises missing