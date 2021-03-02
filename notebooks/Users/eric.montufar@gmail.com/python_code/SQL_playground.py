# Databricks notebook source
# MAGIC %md 
# MAGIC # Notebook to test SQL queries 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- top 10 elements by category using rank 
# MAGIC SELECT rs.Field1,rs.Field2 
# MAGIC     FROM (
# MAGIC         SELECT Field1,Field2, Rank() 
# MAGIC           over (Partition BY Section
# MAGIC                 ORDER BY RankCriteria DESC ) AS Rank
# MAGIC         FROM table
# MAGIC         ) rs WHERE Rank <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC Select CountryName from Application.Countries 
# MAGIC  
# MAGIC Declare @val Varchar(MAX); 
# MAGIC Select @val = COALESCE(@val + ', ' + CountryName, CountryName) 
# MAGIC         From Application.Countries Select @val;

# COMMAND ----------

# doing left anti join using spark dataframes
df.as("table1").join(
  df2.as("table2"),
  $"table1.name" === $"table2.name" && $"table1.age" === $"table2.howold",
  "leftanti"
)

# COMMAND ----------

# left anti join using spark sql 
spark.sql(
  """SELECT table1.* FROM table1
    | LEFT ANTI JOIN table2
    | ON table1.name = table2.name AND table1.age = table2.howold
  """.stripMargin)