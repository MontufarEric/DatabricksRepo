# Databricks notebook source


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

