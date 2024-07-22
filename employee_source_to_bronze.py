# Databricks notebook source
# MAGIC %run /Shared/source_to_bronze/utils

# COMMAND ----------

employee_df = read_csv("dbfs:/FileStore/practice/Employee.csv")
department_df = read_csv("dbfs:/FileStore/practice/Department.csv")
country_df = read_csv("dbfs:/FileStore/practice/Country.csv")

# COMMAND ----------

employee_df.display()
department_df.display()
country_df.display()

# COMMAND ----------

write_csv(employee_df,"dbfs:/FileStore/practice/source_to_bronze/employee.csv")
write_csv(department_df,"dbfs:/FileStore/practice/source_to_bronze/department.csv")
write_csv(country_df,"dbfs:/FileStore/practice/source_to_bronze/country.csv")

# COMMAND ----------


