// Databricks notebook source
// MAGIC %md
// MAGIC ### DataFrames Basics
// MAGIC This is a notebook to apply the basic concepts of dataframes in Databricks. 

// COMMAND ----------

// MAGIC %md
// MAGIC Dataframes are tables stored in a distributed way across a spark cluster. Each partition contains the schema(Column names and types) and data rows. All the rows in a DF have the same structure.

// COMMAND ----------

val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("s3a://filestoragedatabricks/Spark-essentials-data/cars.json")

// COMMAND ----------

  // showing a DF
  firstDF.show()


// COMMAND ----------

// showing the schema  
firstDF.printSchema()

// COMMAND ----------

// take returns a list of rows so you can iterate over them 
firstDF.take(10).foreach(println)

// COMMAND ----------

 // definign the schema instead of infering it
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

// COMMAND ----------

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("s3a://filestoragedatabricks/Spark-essentials-data/cars.json")


// COMMAND ----------

// create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

// COMMAND ----------

// create DF from a sequence of tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )

// COMMAND ----------

// creating the DF from the previous tuples. here the schema is inferred 
val manualCarsDF = spark.createDataFrame(cars) 

// COMMAND ----------

// create DFs with implicits --> this allows us to set column names
// Importing implicits  from the spark session

  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")


// COMMAND ----------

 manualCarsDF.printSchema()

// COMMAND ----------

manualCarsDFWithImplicits.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating and Loading Dataframes

// COMMAND ----------

// MAGIC %md
// MAGIC Here We create a Dataframe by hand 

// COMMAND ----------

val cellPhones = Seq(
  ('Samsung' 64, 2020, 5 ),
  ('Apple', 32, 2020, 3),
  ('Nokia', 8, 2015, 1)
  )

// COMMAND ----------

val cellPhoneDF = cellPhones.toDF("Company", "Memory", "Year", "Cameras")

// COMMAND ----------

cellPhonesDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Here we read another Dataframe from an S3 bucket and print its schema and the number of rows 

// COMMAND ----------

val bandsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("s3a://filestoragedatabricks/Spark-essentials-data/bands.json")

// COMMAND ----------

bandsDF.printSchema()

// COMMAND ----------

print(s"the DF has ${bandsDF.count()} rows ")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Reading a writing dataframes from different data sources

// COMMAND ----------

// MAGIC %md
// MAGIC In order to read a Dataframe we need the following: 
// MAGIC - format --> json, csv, parquet (default), ...
// MAGIC - schema --> optional to add schema or inferSchema true
// MAGIC - option --> zero or more to modify the reading 
// MAGIC - load   --> path to the file (you can also add the path as an option)
// MAGIC 
// MAGIC options: mode (what to do in case of malformed rows) 
// MAGIC          - inferSchema
// MAGIC          - header
// MAGIC          - dateFormat --> to use a specific date type when the datetype in given schema
// MAGIC          - allowSingelquotes --> allow single qoutes in json
// MAGIC          - compression --> to read a compressed files bzip2, gzip, lz4, snappy, ...
// MAGIC          - sep -->  to specify a separator for CSV files ("," "\t" ...)
// MAGIC          - nullValue --> to identify null values in CSV files
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC --> you can also pass options in plural to the DF and create a map to pass multipla options
// MAGIC 
// MAGIC --> Instead of format(json) we can use json instead of read 
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html

// COMMAND ----------

val bandsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("path","s3a://filestoragedatabricks/Spark-essentials-data/bands.json")
    .load()

// COMMAND ----------

// MAGIC %md
// MAGIC To write a dataframe the structure is almost the same. 
// MAGIC - format --> json, csv, ...
// MAGIC - save mode --> overwrite, append, ignore, ...
// MAGIC - options
// MAGIC - save --> we can add the path here or as an option
// MAGIC 
// MAGIC While saving from spark, we save it in a distributed way. That is, it saves the partitions in a folder. 

// COMMAND ----------

bandsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path","s3a://filestoragedatabricks/Spark-essentials-data/bands2.json")
    .save()

// COMMAND ----------

// MAGIC %md 
// MAGIC to connect to a Database is the same procedure, but here we have to add some options with the connection parameters. 
// MAGIC 
// MAGIC this can be used to read and write

// COMMAND ----------

// reading from a remote db
val dfDatabase = spark.read
                .format("jdbc")
                .option("driver","org.postgresql.Driver")
                .option("url","jdbc:postgresql:///localhost:7070/databaseName")
                .option("user","yourUser")
                .option("password","yourPassword")
                .option("dbtable","yourTableName")
                .load()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Processing dataframes

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Columnwise transformations

// COMMAND ----------

val carsDF = spark.read
    .option("inferSchema", "true")
    .json("s3a://filestoragedatabricks/Spark-essentials-data/cars.json")

// COMMAND ----------

carsDF.show()

// COMMAND ----------

// Creating a column
val firstCol = carsDF.col("Name")

// COMMAND ----------



// COMMAND ----------

