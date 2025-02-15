# Databricks notebook source
df=spark.read.format("parquet")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@carazureproject.dfs.core.windows.net/rawdata")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, split
df=df.withColumn("model_category", split(col("Model_ID"), "-")[0])
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df.withColumn("Units_sold",col("Units_Sold").cast(StringType())).printSchema()

# COMMAND ----------

df=df.withColumn("RevPerUnit",col("Revenue")/col("Units_Sold"))
display(df)

# COMMAND ----------

df=df.dropDuplicates(["Branch_ID","Dealer_ID","Model_ID","Date_ID"])
display(df) 

# COMMAND ----------

df.groupBy("Year","BranchName").agg(sum("units_sold").alias("Total_Units")).sort('Year','Total_units',ascending=[1,0]).display()

# COMMAND ----------

df.write.format("parquet").mode("append")\
    .option('path','abfss://silver@carazureproject.dfs.core.windows.net/carsales').save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet. `abfss://silver@carazureproject.dfs.core.windows.net/carsales`

# COMMAND ----------

