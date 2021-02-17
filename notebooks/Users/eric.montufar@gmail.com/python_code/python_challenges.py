# Databricks notebook source
# MAGIC %md
# MAGIC ## Python code challenges
# MAGIC Solving hackerrank code challenges with python 

# COMMAND ----------

# the time in words problem
# https://www.hackerrank.com/challenges/the-time-in-words/problem

# COMMAND ----------

#!/bin/python3

import math
import os
import random
import re
import sys

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