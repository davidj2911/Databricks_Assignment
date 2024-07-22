# Databricks notebook source
# MAGIC %run "/Shared/source_to_bronze/utils"

# COMMAND ----------

emp_df = spark.read.format("delta").load("dbfs:/FileStore/practice/silver/Employee_info/dim_employee")
emp_df.display()

# COMMAND ----------

emp_df.createOrReplaceTempView("temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select salary,department from temp_view
# MAGIC group by department, salary
# MAGIC order by department, salary desc

# COMMAND ----------

empCount = emp_df.groupBy("country","department").agg(count("department").alias("emp_count"))
empCount.display()

# COMMAND ----------



# COMMAND ----------

dept_name_df = department_df.join(employee_df, department_df.DepartmentID == employee_df.Department, "inner")
country_name_df = dept_name_df.join(country_df, dept_name_df.Country == country_df.CountryCode, "inner")
result = (country_name_df.select("DepartmentName","CountryName")).display()

# COMMAND ----------

avgAge_per_dept = (employee_df.groupBy("Department").agg(avg("Age"))).display()

# COMMAND ----------

emp_df.display()

# COMMAND ----------

(add_Column(emp_df,"at_load_date")).display()

# COMMAND ----------

emp_df.write.format("delta").mode("overwrite").option("replaceWhere", "col_name = '2024-07-22'").save("dbfs:/FileStore/practice/gold/employee/table/fact_employee")

# COMMAND ----------

final_result = spark.read.format("delta").load("dbfs:/FileStore/practice/gold/employee/table/fact_employee")
final_result.display()

# COMMAND ----------


