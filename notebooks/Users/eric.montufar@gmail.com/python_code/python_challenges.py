# Databricks notebook source
# MAGIC %md
# MAGIC ## Python code challenges
# MAGIC Solving hackerrank code challenges with python 

# COMMAND ----------

# the time in words problem
# https://www.hackerrank.com/challenges/the-time-in-words/problem
#challenges

# COMMAND ----------

#!/bin/python3

import math
import os
import random
import re
import sys
import numpy as np

# Complete the timeInWords function below.
def timeInWords(h, m):
    dc_h = {
     0 : 'zero'
    ,1 : 'one'
    ,2 : 'two'
    ,3 : 'three'
    ,4 : 'four'
    ,5 : 'five'
    ,6 : 'six'
    ,7 : 'seven'
    ,8 : 'eight'
    ,9 : 'nine'
    ,10 : 'ten'
    ,11 : 'eleven'
    ,12 : 'twelve'}
    
    dc_m = {
     1 : 'one minute'
    ,2 : 'two minutes'
    ,3 : 'three minutes'
    ,4 : 'four minutes'
    ,5 : 'five minutes'
    ,6 : 'six minutes'
    ,7 : 'seven minutes' 
    ,8 : 'eight minutes'
    ,9 : 'nine minutes'
    ,10 : 'ten minutes'
    ,11 : 'eleven minutes'
    ,12 : 'twelve minutes'
    ,13 : 'thirteen minutes'
    ,14 : 'fourteen minutes'
    ,15 : 'quarter'
    ,16 : 'sixteen minutes'
    ,17 : 'seventeen minutes'
    ,18 : 'eighteen minutes'
    ,19 : 'nineteen minutes'
    ,20 : 'twenty minutes'
    ,21 : 'twenty one minutes'
    ,22 : 'twenty two minutes'
    ,23 : 'twenty three minutes'
    ,24 : 'twenty four minutes'
    ,25 : 'twenty five minutes' 
    ,26 : 'twenty six minutes'
    ,27 : 'twenty seven minutes'
    ,28 : 'twenty eight minutes'
    ,29 : 'twenty nine minutes'
    ,30 : 'half'

}
    if m==0:
        return(dc_h[h] +  " o' clock")

    else:
        if m<=30 :
            return(dc_m[m] + ' past ' + dc_h[h])
        else:
            return(dc_m[60 - m] + ' to ' + dc_h[h+1]) 
        
if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    h = int(input())

    m = int(input())

    result = timeInWords(h, m)

    fptr.write(result + '\n')

    fptr.close()

# COMMAND ----------

# Greedy florist 
# https://www.hackerrank.com/challenges/greedy-florist/problem

# COMMAND ----------

#!/bin/python3

import math
import os
import random
import re
import sys

# Complete the getMinimumCost function below.
def getMinimumCost(k, c):
    cost = 0
    c = sorted(c, reverse=True)
    for i in range(0, len(c)):
        cost += (i // k + 1) * c[i]
    return cost
    
if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    nk = input().split()

    n = int(nk[0])

    k = int(nk[1])

    c = list(map(int, input().rstrip().split()))

    minimumCost = getMinimumCost(k, c)

    fptr.write(str(minimumCost) + '\n')

    fptr.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mask Functions Spark
# MAGIC 
# MAGIC UDFs to mask the first digits of the SSN and masking email address

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os

def mask_func(colVal):
    if len(colVal)==9:
        charList=list(colVal)
        charList[:5]='*'*5
        return "".join(charList)
    else:
        return colVal


# COMMAND ----------

df=spark.read.csv('your_CSV' ,header=True)
df.show(2)
mask_func_udf = udf(mask_func, StringType())
df_masked=df.withColumn("Column_masked",mask_func_udf(df["Columnr"]))
df_masked=df_masked.drop("Column").withColumnRenamed("Column_masked","Column")
print ("Masked Data: \n")
df_masked.show(2)



# COMMAND ----------

def mask_email(email):
    import re
    return re.replace(r'\w+(?=.{0}@)', 'x', email)




# COMMAND ----------

spark.udf.register("mask_email", StringType())

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dev_db.file_process_log;
# MAGIC create table dev_db.file_process_log
# MAGIC (
# MAGIC file_id                 string     comment "none",
# MAGIC filename                string     comment "none",
# MAGIC processing_timestamp    timestamp  comment "none",
# MAGIC processing_status       string     comment "none"
# MAGIC )
# MAGIC using delta
# MAGIC options(path="s3a://filestoragedatabricks/parquet_files/tables2/file_process_log")

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists dev_db.restaurants;
# MAGIC CREATE TABLE dev_db.restaurants (
# MAGIC  business_id int,
# MAGIC  business_name string,
# MAGIC  business_address string,
# MAGIC  business_postal_code string
# MAGIC     )
# MAGIC using delta
# MAGIC options(path="s3a://filestoragedatabricks/parquet_files/tables2/restaurants")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dev_db.restaurants ('business_id', 'business_name', 'business_address', 'business_postal_code') VALUES
# MAGIC  (484, 'Columbus Cafe', '562 Green St', '94133'),
# MAGIC  (65382, 'Manna', '845 Irving St', '94122'),
# MAGIC  (19131, 'New Casa Maria', '1201 So. Van Ness Ave.', '94110'),
# MAGIC  (3678, 'ROUND TABLE PIZZA', '737 PORTOLA Dr', '94127'),
# MAGIC  (82635, 'Epicurean at Dolby', '1275 Market St', '94103'),
# MAGIC  (65271, 'California Pacific Medical Ctr - Hospital Kitchen', '2333 BuchaNULL St Level A', '94120'),
# MAGIC  (90951, 'Saigon BBQ Restaurant', '2623 San Bruno Ave.', '94134'),
# MAGIC  (4449, 'Universal Bakery Inc.', '3458 MISSION St', '94110'),
# MAGIC  (1154, 'SUNFLOWER RESTAURANT', '506 Valencia St', '94103'),
# MAGIC  (18940, "Bruno's", '2389 MISSION St', '94110'),
# MAGIC  (66771, 'Stanza', '1673 Haight St', '94117'),
# MAGIC  (88702, 'Dancing Bull', '4217 Geary Blvd', '94118'),
# MAGIC  (66732, 'Soma Restaurant And Bar', '85 05th St', '94103'),
# MAGIC  (1613, 'BRICK HOUSE CAFE', '426 BRANNAN St', '94107'),
# MAGIC  (4798, 'M & M SHORTSTOP', '2145 GENEVA Ave', '94134'),
# MAGIC  (69849, "Darren's Cafe", '2731 Taylor St', '94133'),
# MAGIC  (67815, 'Southpaw BBQ', '2170 MISSION St', '94110'),
# MAGIC  (59727, "Luigi's Pizzeria", 'Pier 39  Space 211-212', '94133'),
# MAGIC  (74763, 'Aquitaine LLC', '175 Sutter St', '94104'),
# MAGIC  (95231, '95231 Doggie Diner Cart', '24 Willie Mays Pl View Lvl Sect 307', '94107'),
# MAGIC  (86386, 'Fresh Meat Seafood Market', '2704 Mission St', '94110'),
# MAGIC  (3115, "Yee's Restaurant", '1131 Grant Ave', '94133'),
# MAGIC  (69606, 'El Porteno I Restaurant', '5173 MISSION St', '94112'),
# MAGIC  (88688, 'Akira Japanese Restaurant', '1634 Bush St', '94109'),
# MAGIC  (97261, 'KAIYO RESTAURANT & BAR', '1838 UNION St', '94123'),
# MAGIC  (78389, 'Bar Oda', '1500 Owens St', '94107'),
# MAGIC  (76958, '7 Eleven #2366-35722A', '4850 Geary Blvd', '94118'),
# MAGIC  (3661, 'LA CUMBRE', '515 VALENCIA St', '94110'),
# MAGIC  (80773, 'Round Table Pizza', '4523 Mission St', '94112'),
# MAGIC  (63597, 'Zero Zero', '826 FOLSOM St', '94107'),
# MAGIC  (1565, "Uncle Vito's Pizza", '700 Bush St', '94108'),
# MAGIC  (3167, "MING'S DINER", '2129 Taraval St', '94116'),
# MAGIC  (91501, 'Jicama, LLC', '103 Horne Ave.', '94124'),
# MAGIC  (82721, 'Bon Appetit @ Twitter', '875 Stevenson St 10th Floor', '94103'),
# MAGIC  (90039, 'Grill Restaurant & Lobby Bar', '125 3rd St.', '94103'),
# MAGIC  (34144, 'Pizza Joint', '2414 San Bruno Ave', '94134'),
# MAGIC  (97558, 'VEGAN-N-KABOB', '1109 Fillmore St', '94115');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inspections (
# MAGIC  inspection_id varchar(64),
# MAGIC  inspection_date date,
# MAGIC  inspection_score integer,
# MAGIC  inspection_type varchar(64),
# MAGIC  risk_category varchar(64),
# MAGIC  business_id integer
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO inspections ('inspection_id', 'inspection_date', 'inspection_score', 'inspection_type', 'risk_category', 'business_id') VALUES
# MAGIC  ('88702_20180926', '2018-09-26', 86.0, 'Routine - Unscheduled', 'Moderate Risk', 88702),
# MAGIC  ('35786_20170210', '2017-02-10', 92.0, 'Routine - Unscheduled', 'Low Risk', 35786),
# MAGIC  ('67815_20180920', '2018-09-20', 85.0, 'Routine - Unscheduled', 'Low Risk', 67815),
# MAGIC  ('90951_20170419', '2017-04-19', NULL, 'New Ownership', 'Low Risk', 90951),
# MAGIC  ('97558_20180912', '2018-09-12', NULL, 'New Ownership', NULL, 97558),
# MAGIC  ('14971_20160919', '2016-09-19', 96.0, 'Routine - Unscheduled', 'Moderate Risk', 14971),
# MAGIC  ('88688_20180724', '2018-07-24', 90.0, 'Routine - Unscheduled', 'Moderate Risk', 88688),
# MAGIC  ('86386_20161003', '2016-10-03', NULL, 'Reinspection/Followup', NULL, 86386),
# MAGIC  ('1613_20180426', '2018-04-26', NULL, 'Complaint', 'Low Risk', 1613),
# MAGIC  ('78389_20180321', '2018-03-21', 90.0, 'Routine - Unscheduled', 'Moderate Risk', 78389),
# MAGIC  ('95231_20180316', '2018-03-16', NULL, 'New Ownership', NULL, 95231),
# MAGIC  ('69606_20160727', '2016-07-27', NULL, 'Reinspection/Followup', NULL, 69606),
# MAGIC  ('3167_20161107', '2016-11-07', 78.0, 'Routine - Unscheduled', 'Moderate Risk', 3167),
# MAGIC  ('19171_20160105', '2016-01-05', 94.0, 'Routine - Unscheduled', 'Moderate Risk', 19171),
# MAGIC  ('1154_20160912', '2016-09-12', 76.0, 'Routine - Unscheduled', 'Low Risk', 1154),
# MAGIC  ('484_20160711', '2016-07-11', 91.0, 'Routine - Unscheduled', 'High Risk', 484),
# MAGIC  ('74763_20171027', '2017-10-27', 88.0, 'Routine - Unscheduled', 'Low Risk', 74763),
# MAGIC  ('484_20160711', '2016-07-11', 91.0, 'Routine - Unscheduled', 'Low Risk', 484),
# MAGIC  ('69606_20160720', '2016-07-20', 85.0, 'Routine - Unscheduled', 'Moderate Risk', 69606),
# MAGIC  ('3678_20180725', '2018-07-25', 96.0, 'Routine - Unscheduled', 'Low Risk', 3678),
# MAGIC  ('5310_20160603', '2016-06-03', NULL, 'Reinspection/Followup', 'Moderate Risk', 5310),
# MAGIC  ('3115_20160613', '2016-06-13', NULL, 'Complaint', 'Low Risk', 3115),
# MAGIC  ('1565_20160826', '2016-08-26', 79.0, 'Routine - Unscheduled', 'Low Risk', 1565),
# MAGIC  ('77425_20171012', '2017-10-12', 88.0, 'Routine - Unscheduled', 'Low Risk', 77425),
# MAGIC  ('59727_20180821', '2018-08-21', 82.0, 'Routine - Unscheduled', 'Low Risk', 59727),
# MAGIC  ('88090_20180530', '2018-05-30', 88.0, 'Routine - Unscheduled', 'Moderate Risk', 88090),
# MAGIC  ('91987_20170725', '2017-07-25', NULL, 'New Ownership', NULL, 91987),
# MAGIC  ('80773_20161110', '2016-11-10', 91.0, 'Routine - Unscheduled', 'Low Risk', 80773),
# MAGIC  ('95231_20180326', '2018-03-26', 92.0, 'Routine - Unscheduled', 'Moderate Risk', 95231),
# MAGIC  ('4449_20161215', '2016-12-15', 80.0, 'Routine - Unscheduled', 'Low Risk', 4449),
# MAGIC  ('65382_20180905', '2018-09-05', 81.0, 'Routine - Unscheduled', 'Moderate Risk', 65382),
# MAGIC  ('38442_20170227', '2017-02-27', NULL, 'Reinspection/Followup', NULL, 38442),
# MAGIC  ('66732_20160627', '2016-06-27', 81.0, 'Routine - Unscheduled', 'Moderate Risk', 66732),
# MAGIC  ('91501_20170822', '2017-08-22', NULL, 'New Ownership', NULL, 91501),
# MAGIC  ('66771_20170207', '2017-02-07', 98.0, 'Routine - Unscheduled', 'Low Risk', 66771),
# MAGIC  ('19131_20160105', '2016-01-05', 88.0, 'Routine - Unscheduled', 'Low Risk', 19131),
# MAGIC  ('79821_20170612', '2017-06-12', 75.0, 'Routine - Unscheduled', 'Low Risk', 79821),
# MAGIC  ('5848_20180516', '2018-05-16', 100.0, 'Routine - Unscheduled', NULL, 5848),
# MAGIC  ('3522_20161205', '2016-12-05', NULL, 'Complaint', 'Low Risk', 3522),
# MAGIC  ('4798_20170925', '2017-09-25', NULL, 'Reinspection/Followup', NULL, 4798);

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE @Names VARCHAR(8000) 
# MAGIC SELECT @Names = COALESCE(@Names + ', ', '') + Name 
# MAGIC FROM People

# COMMAND ----------

# MAGIC %sql
# MAGIC use dev_db;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dev_db.file_process_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE @files VARCHAR(8000) 
# MAGIC SELECT @files = COALESCE(@files + ', ', '') + file_id
# MAGIC FROM dev_db.file_process_log

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Subset Sum Problem

# COMMAND ----------

# A recursive solution for subset sum
# problem
 
# Returns true if there is a subset
# of set[] with sun equal to given sum
def isSubsetSum(set, n, sum):
    # Base Cases
    if (sum == 0):
        return True
    if (n == 0):
        return False
 
    # If last element is greater than
    # sum, then ignore it
    if (set[n - 1] > sum):
        return isSubsetSum(set, n - 1, sum)
 
    # else, check if sum can be obtained
    # by any of the following
    # (a) including the last element
    # (b) excluding the last element
    return isSubsetSum(
        set, n-1, sum) or isSubsetSum(
        set, n-1, sum-set[n-1])


# COMMAND ----------

# Driver code
set = [3, 34, 4, 12, 5, 2]
sum = 9
n = len(set)
if (isSubsetSum(set, n, sum) == True):
    print("Found a subset with given sum")
else:
    print("No subset with given sum")


# COMMAND ----------

## find the dupplicate element in an array 

def findDup(arr):
  l = []
  for i in arr:
    if i in l:
      return i
    else: 
      l.append(i)
  return "no dulicates"

# COMMAND ----------

## move all the zeros from an array to the right keeping the order 
## [0,2,4,5,0,8,0,9]  --> [2,4,5,8,9,0,0,0]
def moveZero(arr):
  l = []
  c = 0
  for i in arr:
    if i != 0:
      l.append(i)
    else:
      c+=1
  for i in range(c):
    l.append(0)
  return l