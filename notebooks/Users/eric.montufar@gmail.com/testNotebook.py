# Databricks notebook source


# COMMAND ----------

import urllib
SECRET_KEY = ""
ACCESS_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")

AWS_BUCKET_NAME = "filestoragedatabricks"
MOUNT_NAME = "MyS3Bucket"



# COMMAND ----------

ACCESS_KEY = dbutils.secrets.get(scope = "awskeys", key = "accesskey")
SECRET_KEY = dbutils.secrets.get(scope = "awskeys", key = "secretkey")
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "tables-bucket-databricks-1"
MOUNT_NAME = "tablesbucket"


# COMMAND ----------

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/MyS3Bucket"))

# COMMAND ----------

#this is a test 

# COMMAND ----------

df = spark.read.load("s3a://filestoragedatabricks/Iris.csv")

# COMMAND ----------

AWS_BUCKET_NAME = "databricksmasterbucket-01"
MOUNT_NAME = "MyBucket"
dbutils.fs.mount("s3a://%s" % AWS_BUCKET_NAME, "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

# COMMAND ----------

df = spark.read.csv("dbfs:/s3bucket/iris.csv")

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls /FileStore/shared_uploads/eric.montufar@gmail.com/

# COMMAND ----------

df = spark.read.option('inferschema','True').option('header','True').csv("dbfs:/FileStore/shared_uploads/eric.montufar@gmail.com/Iris.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("negativeSepalLenght", df["SepalLengthCm"] * -1)

# COMMAND ----------

df.take(5)

# COMMAND ----------

df.write.mode("overwrite").option("header","true").csv("dbfs:/FileStore/sharedFiles/Iris_test.csv")

# COMMAND ----------

# MAGIC 
# MAGIC %pip install bs4

# COMMAND ----------

from bs4 import BeautifulSoup


# COMMAND ----------

soup = BeautifulSoup("<p>Some<b>bad<i>HTML")

# COMMAND ----------

soup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dev_db COMMENT 'This is a development database' LOCATION '/mnt/tablesbucket/'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dev_db.processing_log;
# MAGIC create table dev_db.processing_log
# MAGIC (
# MAGIC file_id                 string     comment "none",
# MAGIC filename                string     comment "none",
# MAGIC processing_timestamp    timestamp  comment "none",
# MAGIC processing_status       string     comment "none"
# MAGIC )
# MAGIC using delta
# MAGIC options(path="/mtn/tablesbucket/")
# MAGIC partitioned by (file_id)

# COMMAND ----------

spark.sql("""INSERT INTO dev_db.processing_log2  VALUES ('12312', 'archivo.txt', '12-12-2020', 'passed');""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dev_db.processing_log 

# COMMAND ----------

dbutils.fs.ls("s3a://filestoragedatabricks")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dev_db.processing_log2;
# MAGIC create table dev_db.processing_log2
# MAGIC (
# MAGIC file_id                 string     comment "none",
# MAGIC filename                string     comment "none",
# MAGIC processing_timestamp    timestamp  comment "none",
# MAGIC processing_status       string     comment "none"
# MAGIC )
# MAGIC using delta
# MAGIC options(path="s3a://filestoragedatabricks/tables-dev/")
# MAGIC partitioned by (file_id)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dev_db.processing_log2 

# COMMAND ----------

ACCESS_KEY = dbutils.secrets.get(scope = "awskeys", key = "accesskey")
SECRET_KEY = dbutils.secrets.get(scope = "awskeys", key = "secretkey")
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
myRDD = sc.textFile("s3a://%s:%s@%s/tables-bucket-databricks-1/test-tables" % ACCESS_KEY, ENCODED_SECRET_KEY, BUCKET_NAME)
myRDD.count()

# COMMAND ----------

# dbutils.fs.rm("/FileStore/tables/your_table_name.csv")
dbutils.fs.ls("/mtn/tablesbucket/")


# COMMAND ----------

df1 = spark.read.option('inferschema','True').option('header','True').csv("s3a://filestoragedatabricks/Iris.csv")
df2 = spark.read.option('inferschema','True').option('header','True').csv("s3a://filestoragedatabricks/Iris.csv")

# COMMAND ----------



# COMMAND ----------

df.printSchema()

# COMMAND ----------



# COMMAND ----------

from collections import namedtuple  
      
# Declaring namedtuple()   
Student = namedtuple('Student',['name','age','DOB'])   
      
# Adding values   
S = Student('Nandini','19','2541997')   
      
# Access using index   
print ("The Student age using index is : ",end ="")   
print (S[1])   
      
# Access using name    
print ("The Student name using keyname is : ",end ="")   
print (S.name) 

# COMMAND ----------

user_row = namedtuple('user_row', 'dob age is_fan'.split())
data = [
    user_row('1990-05-03', 29, True),
    user_row('1994-09-23', 25, False)
]

# COMMAND ----------



# COMMAND ----------

def restock(itemCount, target):
    cont=0
    for item in itemCount:
        cont += item
        if cont >= target:
            return abs(target - cont)
        
    return abs(target - cont)



# COMMAND ----------

def comparatorValue2(a, b, d):
    s = 0
    for j in a:
        lista1 = []
        for i in b:
            lista1.append(abs(j-i) > d)
        s+= all(lista1)

    return s

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dev_db.months;
# MAGIC create table dev_db.months
# MAGIC (
# MAGIC file_id              string     comment "none",
# MAGIC month                string     comment "none",
# MAGIC col1                 int        comment "none"
# MAGIC )
# MAGIC using delta
# MAGIC options(path="s3a://filestoragedatabricks/tables-dev/")
# MAGIC partitioned by (file_id)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

