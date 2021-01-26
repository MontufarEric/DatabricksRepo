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

import org.apache.spark.sql.functions._

// counting in two different ways
val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null

// with expr
moviesDF.selectExpr("count(Major_Genre)").show()


// COMMAND ----------

// counting all
moviesDF.select(count("*"))show() // count all the rows, and will INCLUDE nulls

// COMMAND ----------

// counting distinct
moviesDF.select(countDistinct(col("Major_Genre"))).show()

// COMMAND ----------

 // approximate count for qick data analysis (for real big data) 
// approx is part of spark.sql.functions

import org.apache.spark.sql.functions._
moviesDF.select(approx_count_distinct(col("Major_Genre"))).show

// COMMAND ----------

// min and max
val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
moviesDF.selectExpr("min(IMDB_Rating)").show

// COMMAND ----------

 // sum
moviesDF.select(sum(col("US_Gross"))).show
moviesDF.selectExpr("sum(US_Gross)").show

// COMMAND ----------

  // avg
moviesDF.select(avg(col("Rotten_Tomatoes_Rating"))).show()
moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

// COMMAND ----------

// data science
moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show

// COMMAND ----------

// MAGIC %md
// MAGIC ## Grouping

// COMMAND ----------

// Group by count --> Relational Grouped dataset

val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count()  // select count(*) from moviesDF group by Major_Genre
countByGenreDF.show

// COMMAND ----------

// Group by  avg

val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
avgRatingByGenreDF.show

// COMMAND ----------

// Group by using agg --> custom aggregations  + Order by

val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

aggregationsByGenreDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Aggregations Practice

// COMMAND ----------

moviesDF.printSchema

// COMMAND ----------

val profitsDF = moviesDF.select(sum(col("Worldwide_Gross")))
profitsDF.show()

// COMMAND ----------

moviesDF.select(countDistinct(col("Director"))).show()

// COMMAND ----------

moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

// COMMAND ----------

val aggregationsByDirectorDF = moviesDF
    .groupBy(col("Director"))
    .agg(
      avg("US_Gross").as("avg_US_gross_revenue"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Director"))
aggregationsByDirectorDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC # Dataframe Joins

// COMMAND ----------

// Reading DF1

val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("s3a://filestoragedatabricks/Spark-essentials-data/guitars.json")

guitarsDF.show

// COMMAND ----------

// Reading DF2
val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("s3a://filestoragedatabricks/Spark-essentials-data/bands.json")

bandsDF.show

// COMMAND ----------

// Reading DF2
val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("s3a://filestoragedatabricks/Spark-essentials-data/guitarPlayers.json")

guitaristsDF.show

// COMMAND ----------

// inner joins
// here we perform a join based on a predefined condition

val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

// COMMAND ----------

// outer joins
// left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing

guitaristsDF.join(bandsDF, joinCondition, "left_outer").show

// COMMAND ----------

// right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

// COMMAND ----------

// outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
guitaristsDF.join(bandsDF, joinCondition, "outer").show()

// COMMAND ----------

// semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
// like a fancy filtering 


guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

// COMMAND ----------

// anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
guitaristsDF.join(bandsDF, joinCondition, "left_anti").show

// COMMAND ----------

// things to bear in mind to avoid duplicate columns while joining
// guitaristsBandsDF.select("id", "band").show // this crashes

// option 1 - rename the column on which we are joining
guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band").show

// COMMAND ----------

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id")).show

// COMMAND ----------

// option 3 - rename the offending column and keep the data
val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId")).show


// COMMAND ----------

// using complex types
guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show


// COMMAND ----------

// MAGIC %md
// MAGIC ### spliting a column into many 

// COMMAND ----------

// collection of tupples

val data = Seq(("James, A, Smith","2018","M",3000),
    ("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),
    ("Maria,Anne,Jones","2005","F",4000),
    ("Jen,Mary,Brown","2010","",-1)
  )

  import spark.sqlContext.implicits._
  val df = data.toDF("name","dob_year","gender","salary")
  df.printSchema()
  df.show(false)

// COMMAND ----------

// splitting a column into many

 val df2 = df.select(split(col("name"),",").getItem(0).as("FirstName"),
    split(col("name"),",").getItem(1).as("MiddleName"),
    split(col("name"),",").getItem(2).as("LastName"))
    .drop("name")


// COMMAND ----------



// COMMAND ----------

