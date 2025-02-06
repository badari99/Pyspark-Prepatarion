# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("interviewprep1").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Loading Data into Dataframes

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/Employee_sample_data1.csv",header=True,inferSchema=True)
df.show(5)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. salary order with department wise 

# COMMAND ----------

win=Window.partitionBy("department").orderBy(col("salary").desc())
res=df.withColumn("rank",row_number().over(win))
colslist = ["employee_id", "full_name", "job", "department", "salary", "rank"]
res.select(colslist).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. List of employeees details whose Salary is greater than department average salary

# COMMAND ----------

df1=df.groupBy("department").agg(avg(col("salary")).alias("department_avg"))
df1.show()

# COMMAND ----------

df2=df.join(df1,df["department"]==df1["department"]).select(df["*"], df1["department_avg"])
res=df2.filter(col("salary")>=col("department_avg"))
l1 = ["employee_id", "full_name", "job" ,"Salary","department","department_avg"]
res.select(l1).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. List of employeees details whose Salary is less than department average salary

# COMMAND ----------

res1=df2.filter(col("salary")<col("department_avg"))
l1 = ["employee_id", "full_name", "job" ,"Salary","department","department_avg"]
res1.select(l1).show(5)

# COMMAND ----------

Q4. List of employees who worked more than 2 years

# COMMAND ----------

res2=df.withColumn("Hire Date",to_date(col("Hire Date"),"dd-MM-yyyy")).withColumn("work",round(months_between(current_date(),col("Hire Date"))/12,1))
l1 = ["employee_id", "full_name", "job" ,"Salary","department","Hire date","work"]                                                       
res2.select(l1).filter(col("work")>2).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Q5. count of employess department wise except managers

# COMMAND ----------

res3=df.filter(col("job")!="Manager")
res4=res3.groupBy("department").count()
res4.show()

# COMMAND ----------


