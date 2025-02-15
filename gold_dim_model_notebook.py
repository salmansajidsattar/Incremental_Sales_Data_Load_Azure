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
                 select distinct(Model_ID) as Model_ID,model_category from parquet.`abfss://silver@carazureproject.dfs.core.windows.net/carsales` 
                 '''
)
df_src.display()

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
     df_sink=spark.sql('''
                  select dim_model_key,Model_ID,model_category from cars_catalog.gold.dim_model
                ''')
else:
    df_sink=spark.sql('''
                  select   1 as dim_model_key,Model_ID,model_category from parquet.`abfss://silver@carazureproject.dfs.core.windows.net/carsales` where 1=2
                ''')
display(df_sink)

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Model_ID']==df_sink['Model_ID'],'left').select(df_src['Model_ID'],df_src['model_category'],df_sink['dim_model_key'])
display(df_filter)

# COMMAND ----------

from pyspark.sql.functions import col
df_filter_old=df_filter.filter(col('dim_model_key').isNotNull())
df_filter_old.display()

# COMMAND ----------

df_filter_new=df_filter.filter(col('dim_model_key').isNull())

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
                         select max(dim_model_key) as max_value from cars_catalog.gold.dim_model
                         ''').collect()[0][0]+1

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

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

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@carazureproject.dfs.core.windows.net/dim_model")

    delta_tbl.alias('trg').merge(df_final.alias('src'),'trg.Model_ID=src.Model_ID')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_final.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@carazureproject.dfs.core.windows.net/dim_model')\
    .saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model;

# COMMAND ----------

