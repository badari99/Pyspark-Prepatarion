# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("interview prep 2").getOrCreate()

# COMMAND ----------

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/sales_short.csv")
df12=spark.read.csv("/FileStore/tables/sales_short.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Q1.Add current date and current time time stamp

# COMMAND ----------

df1.withColumn("current_date",current_date())\
.withColumn("current_timestamp",current_timestamp())\
.select(col("current_date"),col("current_timestamp"))\
.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. To convert into specific date format

# COMMAND ----------

df3 = df1.withColumn("new_order_date_str", date_format(col("order_date"),"yyyy-MM-dd"))
df3\
    .select("*",
            current_date().alias("current_date"), 
            current_timestamp().alias("current_timestamp")).display()
    

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

Q3. Coverting strings to Date and Time Stamp format

# COMMAND ----------

 df3=df3.withColumn("new_order_date", to_date(col("new_order_date_str"))) \
    .withColumn("order_timestamp", to_timestamp(col("order_date")))

df3.select(col("order_date"),col("new_order_date_str"), col("new_order_date"), col("order_timestamp")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q4. Add pr sub specfic number of days in date colum

# COMMAND ----------

df3.withColumn("add_2_months", add_months(col("new_order_date"), 2))\
    .withColumn("sub_3_months", add_months(col("new_order_date"), -3))\
    .withColumn("add_2_days", date_add(col("new_order_date"), 2))\
    .withColumn("sub_4_days", date_sub(col("new_order_date"), 4))\
    .select("new_order_date", "add_2_months", "sub_3_months", "add_2_days", "sub_4_days")\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q5.  Tp get the differnece between dates

# COMMAND ----------

df3.withColumn("current_date_diff", datediff(current_date(), col("new_order_date")))\
    .select("current_date_diff", current_date().alias("current_date"), "new_order_date")\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q6. To get the differnece between months

# COMMAND ----------

df3.withColumn("current_months_diff", months_between(current_date(), col("new_order_date")))\
    .select(round("current_months_diff").alias("current_months_diff"), current_date().alias("current_date"), "new_order_date")\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q6. tp get the specfic date and month,year

# COMMAND ----------

df3.select(col("new_order_date"),
                # day(col("new_order_date")).alias("day"),
                year(col("new_order_date")).alias("year"),
                quarter(col("new_order_date")).alias("quarter"),
                month(col("new_order_date")).alias("month")
                )\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q7. To get day of week ,month,year,lastdate

# COMMAND ----------

df3.withColumn("dayofweek", dayofweek(col("new_order_date")))\
    .withColumn("dayofmonth", dayofmonth(col("new_order_date")))\
    .withColumn("dayofyear", dayofyear(col("new_order_date")))\
    .withColumn("weekofyear", weekofyear(col("new_order_date")))\
    .withColumn("last_day", last_day(col("new_order_date")))\
    .select("new_order_date", "dayofweek", "dayofmonth", "dayofyear", "weekofyear", "last_day")\
    .display()

# COMMAND ----------


