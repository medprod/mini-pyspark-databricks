# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 0,dataframe created
df = spark.read.load('/FileStore/tables/googleplaystore-3.csv', format='csv', sep=',', header='true', escape='"', inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning
#dropping a few columns that may not be so important.
df = df.drop("size", "Content Rating", "Last Updated", "Android Ver", "Current Ver")

# COMMAND ----------

df.show(1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#converting some column data types

#converts the review column from string to integer type
df = df.withColumn("Reviews", col("Reviews").cast(IntegerType()))

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
#removing + symbol from symbol, $ symbol from price and converting both to integer data types

df = df.withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", ""))\
    .withColumn("Installs", col("Installs").cast(IntegerType()))


# COMMAND ----------

df = df.withColumn("Price", regexp_replace(col("Price"), "[$]", ""))\
    .withColumn("Price", col("Price").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(5)

# COMMAND ----------

# DBTITLE 1,Running SQL Commands
#convert the data frame into a view

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql select * from apps

# COMMAND ----------

# DBTITLE 1,Top 10 Most Reviewed Apps
# MAGIC %sql select App, sum(Reviews) from apps
# MAGIC group by App
# MAGIC order by 2 desc
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Top 10 Most Installed Apps
# MAGIC %sql select App, sum(Installs) From apps
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Top Paid Apps
# MAGIC %sql select App, sum(Price) from apps
# MAGIC where Type='Paid'
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC

# COMMAND ----------


