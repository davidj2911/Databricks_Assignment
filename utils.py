# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


def read_csv(path):
    df = spark.read.csv(path, header = True, inferSchema = True)
    return df

def write_csv(df,path):
    df.write.format("csv").save(path)

def read_custom(data,schema):
    df = spark.read.format("csv").schema(schema).load(data)
    return df

def add_Column(df,col_name):
    df1 = df.withColumn(col_name, current_date())
    return df1




# COMMAND ----------

# Function to convert camelCase to snake_case
def camel_to_snake_column_names(df):
 for column in df.columns:
     new_column_name = ''.join(['_' + c.lower() if c.isupper() and i != 0 else c.lower() for i, c in enumerate(column)])
     df = df.withColumnRenamed(column, new_column_name)
 
 return df

# COMMAND ----------


