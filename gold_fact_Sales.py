# Databricks notebook source
df_silver=spark.sql(
    '''
    select * from parquet.`abfss://silver@carazureproject.dfs.core.windows.net/carsales`
    '''
)
display(df_silver)

# COMMAND ----------

df_dealer=spark.sql("select * from cars_catalog.gold.dim_dealer")
df_branch=spark.sql("select * from cars_catalog.gold.dim_branch")
df_model=spark.sql("select * from cars_catalog.gold.dim_model")
df_date=spark.sql("select * from cars_catalog.gold.dim_date")
display(df_branch)

# COMMAND ----------

df_fact=df_silver.join(
    df_branch,df_silver['Branch_ID']==df_branch['Branch_ID'],how='left')\
    .join(df_dealer,df_silver['Dealer_ID']==df_dealer['Dealer_ID'],how='left')\
    .join(df_model,df_silver['Model_ID']==df_model['Model_ID'],how='left')\
        .join(df_date,df_silver['Date_ID']==df_date['Date_ID'],how='left')\
            .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['RevPerUnit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])

# COMMAND ----------

# display(df_fact)
from pyspark.sql.functions import col
df_duplicates = df_fact.groupBy('dim_branch_key').count().filter(col('count') > 1)
display(df_duplicates)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    deltatable=DeltaTable.forName(spark,'cars_catalog.gold.factsales')
      # Deduplicate the source DataFrame
    deltatable.alias('trg').merge(df_fact.alias('src'),'trg.dim_branch_key=src.dim_branch_key and trg.dim_dealer_key=src.dim_dealer_key and trg.dim_model_key=src.dim_model_key and trg.dim_date_key=src.dim_date_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format('delta')\
    .mode('Overwrite')\
    .option("path",'abfss://gold@carazureproject.dfs.core.windows.net/factsales')\
    .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

from pyspark.sql.functions import col

# Group by the specified keys and count the occurrences
df_duplicates = df_fact.groupBy(
    'dim_branch_key', 'dim_dealer_key', 'dim_model_key', 'dim_date_key'
).count().filter(col('count') > 1)

# Join with the original DataFrame to get the full details of the duplicate rows
df_fact_duplicates = df_fact.join(
    df_duplicates, 
    on=['dim_branch_key', 'dim_dealer_key', 'dim_model_key', 'dim_date_key'], 
    how='inner'
)

# Display the duplicate rows
display(df_fact_duplicates)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales;

# COMMAND ----------

