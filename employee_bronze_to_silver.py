# Databricks notebook source
# MAGIC %run /Shared/source_to_bronze/utils

# COMMAND ----------

emp_schema = StructType([\
                 StructField("EmployeeId",IntegerType()),\
                 StructField("EmployeeName",StringType()),\
                 StructField("Department",StringType()),\
                 StructField("Country", StringType()),\
                 StructField("Salary", IntegerType()),\
                 StructField("Age", IntegerType()),\
    
                        ])

df1 = read_custom("dbfs:/FileStore/practice/source_to_bronze/employee.csv",emp_schema)
df1.display()

# COMMAND ----------

dept_schema = StructType([\
                         StructField("DepartmentId",StringType()),\
                             StructField("DepartmentName",StringType())
    
    
    ])

df2 = read_custom("dbfs:/FileStore/practice/source_to_bronze/department.csv",dept_schema)
df2.display()

# COMMAND ----------

country_schema = StructType([\
                  
                  StructField("CountryCode",StringType()),\
                      StructField("CountryName", StringType())
    
    ])

df3 = read_custom("dbfs:/FileStore/practice/source_to_bronze/country.csv",country_schema)
df3.display()

# COMMAND ----------

df1 = camel_to_snake_column_names(df1)
df2 = camel_to_snake_column_names(df2)
df3 = camel_to_snake_column_names(df3)


# COMMAND ----------

df1.display()
df2.display()
df3.display()



# COMMAND ----------

df1 = add_Column(df1,"load_date")

# COMMAND ----------

df1.display()

# COMMAND ----------

spark.sql("create database Employee_info")


# COMMAND ----------

spark.sql("use Employee_info")

# COMMAND ----------

df1.write.option('path',"dbfs:/FileStore/practice/silver/Employee_info/dim_employee").saveAsTable('dim_employee')

# COMMAND ----------


