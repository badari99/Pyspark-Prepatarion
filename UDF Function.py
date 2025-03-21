# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper , col, lit , transform ,concat
from pyspark.sql.types import BooleanType , StringType 
spark=SparkSession.builder.appName("udf").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC When you want to convert the first alphabet of first and last name :**

# COMMAND ----------

def convert_first_to_upper(x):
    name_list=x.split(" ")
    first_name = name_list[0]
    last_name = name_list[1]
    res = first_name[0].upper() + first_name[1:] + " " + last_name[0].upper() + last_name[1:]
    return res

# COMMAND ----------

print(convert_first_to_upper("sarthak dasgupta"))


# COMMAND ----------

upper_udf = udf(lambda x: convert_first_to_upper(x), StringType())


# COMMAND ----------

columns = ["no","Name"]
data = [("1", "sam smith"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()

# COMMAND ----------

df_result = df_src.withColumn("Name", upper_udf(col("name")))
df_result.display()

# COMMAND ----------

df_src.select(col("no"), upper_udf(col("name")).alias("name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC For a given dataframe find out that if its column - "department" has palidrome values or not

# COMMAND ----------

columns = ["name","department", "salary"]
data = [("Harry", "HR", 12000),
    ("George", "ADA", 23000),
    ("Fred", "TQT", 21000),
    ("Sally", "IT", 25000),
    ("Neel", "SOS", 23000),
    ("Ashish", "IT", 27000)]

df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()

# COMMAND ----------

def is_palindrome(val):
    val_rev = val[::-1]
    print("reverse: ", val_rev)
    return val == val_rev

# COMMAND ----------

palindrome_udf = udf(lambda x: is_palindrome(x), BooleanType())
df_result = df_src.withColumn("is_palindrome", palindrome_udf(col("department")))
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC In the below DF there is a column containing name with some special characters and numbers. Remove these special characters and numbers from name**

# COMMAND ----------

columns = ["no","Name"]
data = [("1", "sa$m sm**ith"),
    ("2", "tr@#ce!y s^mith"),
    ("3", "amy%^ & sand(*ers")]

df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()


# COMMAND ----------

import re
def remove_sc(val):
    return re.sub('[^A-Za-z ]', '', val)

remove_sc_udf = udf(lambda x: remove_sc(x), StringType())
print(remove_sc("Sar5&*th@a(k) Da4%sgupt@a"))

# COMMAND ----------

df_src.select(col("no"), remove_sc_udf(col("name")).alias("name")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC Transform

# COMMAND ----------

simpleData = (("Java",4000,5),
    ("Python", 4600,10),
    ("Scala", 4100,15),
    ("Scala", 4500,15),
    ("PHP", 3000,20),
  )
columns= ["CourseName", "fee", "discount"]

df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

def course_update(df):
    return df.withColumn("CourseName", upper(col("CourseName")))

def fee_after_discount(df):
    return df.withColumn("new_fee", col("fee") - lit(col("fee")*col("discount"))/100)

def discount_amount(df):
    return df.withColumn("discounted_fee", lit(col("fee")-col("new_fee")))

# COMMAND ----------

df_result = df.transform(course_update) \
        .transform(fee_after_discount) \
        .transform(discount_amount)

df_result.display()

# COMMAND ----------

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"]),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"]),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"])
]
df = spark.createDataFrame(data=data,schema=["Name","Languages1","Languages2"])
df.display()

# COMMAND ----------

df.select(transform("Languages1", lambda x: upper(x)).alias("languages1")) \
  .display()


# COMMAND ----------

df.select(transform("Languages1", lambda x: concat(x, lit(" programming"))).alias("languages1")) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC For each

# COMMAND ----------

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
def print_num(num):
    print(num)
rdd.foreach(lambda x: print_num(x))
