# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag=dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC #CREATING DIMENSION MODEL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carazureproject.dfs.core.windows.net/carsales` 

# COMMAND ----------


df_src=spark.sql('''
                 select distinct(Date_ID) as Date_ID from parquet.`abfss://silver@carazureproject.dfs.core.windows.net/carsales` 
                 '''
)
df_src.display()

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
     df_sink=spark.sql('''
                  select dim_date_key,Date_ID from cars_catalog.gold.dim_date
                ''')
else:
    df_sink=spark.sql('''
                  select 1 as dim_date_key,Date_ID from parquet.`abfss://silver@carazureproject.dfs.core.windows.net/carsales` where 1=2
                ''')
display(df_sink)

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Date_ID']==df_sink['Date_ID'],'left').select(df_src['Date_ID'],df_sink['dim_date_key'])
display(df_filter)

# COMMAND ----------

from pyspark.sql.functions import col
df_filter_old=df_filter.filter(col('dim_date_key').isNotNull())
df_filter_old.display()

# COMMAND ----------

df_filter_new=df_filter.filter(col('dim_date_key').isNull())

# COMMAND ----------

df_filter_new.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate Key**

# COMMAND ----------

if incremental_flag=='0':
    max_value=1
else:
    max_value=spark.sql('''
                         select max(dim_date_key) as max_value from cars_catalog.gold.dim_date
                         ''').collect()[0][0]+1
    print(max_value)

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_date_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **create final df-df_filter_old+df_filter_new**

# COMMAND ----------

df_final=df_filter_old.union(df_filter_new)

# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCD TYPE -1 (UPSERT)**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@carazureproject.dfs.core.windows.net/dim_date")

    delta_tbl.alias('trg').merge(df_final.alias('src'),'trg.Date_ID=src.Date_ID')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@carazureproject.dfs.core.windows.net/dim_date')\
    .saveAsTable('cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date;

# COMMAND ----------

