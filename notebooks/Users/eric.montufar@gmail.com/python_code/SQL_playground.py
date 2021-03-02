# Databricks notebook source
# MAGIC %md 
# MAGIC # Notebook to test SQL queries 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rs.Field1,rs.Field2 
# MAGIC     FROM (
# MAGIC         SELECT Field1,Field2, Rank() 
# MAGIC           over (Partition BY Section
# MAGIC                 ORDER BY RankCriteria DESC ) AS Rank
# MAGIC         FROM table
# MAGIC         ) rs WHERE Rank <= 10

# COMMAND ----------

