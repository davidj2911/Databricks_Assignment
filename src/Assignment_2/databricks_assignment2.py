# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests,json


main_schema = StructType([\

               StructField("page", IntegerType()),\
               StructField("per_page", IntegerType()),\
               StructField("total", IntegerType()),\
               StructField("total_pages", IntegerType()),\
               StructField("data", ArrayType(custom_schema)),\
               StructField("support", MapType(StringType(), StringType())),\
 
    
    ])



custom_schema = StructType([\
                   StructField("id", IntegerType()),\
                    StructField("email", StringType()),\
                    StructField("first_name",StringType()),\
                    StructField("last_name", StringType()),\
                    StructField("avatar", StringType())\
    
    
    ])

data_input = requests.get("https://reqres.in/api/users?page=2")
data = data_input.json()

df = spark.createDataFrame([data], schema=main_schema)
df.display()

# COMMAND ----------

df = df.drop("page", "per_page", "total", "total_pages", "support")

# COMMAND ----------

df.display()


# COMMAND ----------

df = df.withColumn("total_data",explode("data"))
df.display()

# COMMAND ----------

df = df.drop("data")
df.display()

# COMMAND ----------

df_flatten = df.withColumn("id",df.total_data.id).withColumn("email",df.total_data.email).withColumn("first_name",df.total_data.first_name).withColumn("last_name", df.total_data.last_name).withColumn("avatar", df.total_data.avatar).drop("total_data")
df_flatten.display()

# COMMAND ----------

df_derived_column = df_flatten.withColumn("site_address", split(df_flatten["email"], "@")[1])
df_derived_column.display()

# COMMAND ----------

df_derived_column = df_derived_column.withColumn("load_date", current_date())
df_derived_column.display()

# COMMAND ----------

spark.sql("create database site_info")
spark.sql("use site_info")

df_derived_column.write.format("delta").mode("overwrite").save("dbfs:/FileStore/practice/site_info/person_info")

# COMMAND ----------

final = spark.read.format("delta").load("dbfs:/FileStore/practice/site_info/person_info")
final.display()

# COMMAND ----------


