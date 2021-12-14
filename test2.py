# this is python file 


import json
import requests
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.functions import year
import random
import datetime
import time



## Reading Dataset airline but a few parts 
airline_source = '/databricks-datasets/airlines/'
fname = airline_source+'part-00000'
df_h = spark.read.csv(fname,header=True,inferSchema=True)
schema = df_h.schema 
airline_source = '/databricks-datasets/airlines/part-01[7-9][7-9][7-9]'
df_all = spark.read.csv(airline_source,header=False,schema = schema)
df_all = df_all.dropna(subset=['Year','Month'])


time.sleep(2.4)

print(df_all.count())

time.sleep(60)

print("Finished")