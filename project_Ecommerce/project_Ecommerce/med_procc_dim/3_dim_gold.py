# Databricks notebook source
# MAGIC %md
# MAGIC #Silver to Gold

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import *
from pyspark.sql import Row

# COMMAND ----------

category_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC #products

# COMMAND ----------

df_products = spark.read.table(f"{category_name}.silver.slv_products")
df_brands = spark.read.table(f"{category_name}.silver.slv_brands")
df_category = spark.read.table(f"{category_name}.silver.slv_category")


# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------

display(spark.sql("select * from v_products"))

# COMMAND ----------

spark.sql(f"USE CATALOG {category_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gold_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS(
# MAGIC     SELECT 
# MAGIC         b.brand_name,
# MAGIC         b.brand_code,
# MAGIC         c.category_name,
# MAGIC         c.category_code
# MAGIC     FROM v_brands b 
# MAGIC     INNER JOIN v_category c
# MAGIC     ON b.category_code = c.category_code
# MAGIC )
# MAGIC SELECT p.product_id,
# MAGIC        p.sku,
# MAGIC        bc.brand_name,
# MAGIC        COALESCE(bc.category_name,"Not Available") AS category_name,
# MAGIC        COALESCE(bc.category_code,"Not Available") AS category_code,
# MAGIC        COALESCE(bc.brand_code,"Not Available") AS brand_code,
# MAGIC        p.color,
# MAGIC        p.size,
# MAGIC        p.material,
# MAGIC        p.weight_grams,
# MAGIC        p.length_cm,
# MAGIC        p.width_cm,
# MAGIC        p.height_cm,
# MAGIC        p.rating_count,
# MAGIC        p._source_file,
# MAGIC        p.ingested_at
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brands_categories bc
# MAGIC ON p.brand_code = bc.brand_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.gold_dim_products

# COMMAND ----------

# MAGIC %md
# MAGIC #customers

# COMMAND ----------

india_regions = {
    'MH' : 'West' , 'GJ' : 'West' , 'RJ' : 'West' , 'KA' : 'South' ,
    'TN' : 'South' , 'TS' : 'South' , 'AP' : 'South' , 'KL' : 'South' ,
    'UP' : 'North' , 'WB' : 'North' , 'DL' : 'North'
}
australian_region = {
    'VIC' : 'SouthEast' , 'WA' : 'West' , 'NSW' : 'East' , 'QLD' : 'NorthEast'
}
uk_region = {
    'ENG' : 'England' , 'WLS' : 'Wales' , 'NIR' : 'Northern Ireland' , 'SCT' : 'Scotland'
}
us_region = {
    'MA' : 'NorthEast' , 'FL' : 'South' , 'NJ' : 'NorthEast' , 'CA' : 'West' , 'NY' : 'NorthEast' ,
    'TX' : 'South'
}
uae_region = {
    "AUH" : "Abu Dhabi", "DU" : "Dubai", "SHJ" : "Sharjah"
}

singapore_region = {
    "SG" : "Singapore"
}

canada_region = {
    "BC" : "West", "AB" : "West", "ON" : "East", "QC" : "East", "NS" : "East", "IL": "Other"
}

country_state_map = {
    "India" : india_regions,
    "Australia" : australian_region,
    "United Kingdom" : uk_region,
    "United States" : us_region,
    "United Arab Emirates" : uae_region,
    "Singapore" : singapore_region,
    "Canada" : canada_region
}



# COMMAND ----------

country_state_map

# COMMAND ----------

rows = []

for country, state in country_state_map.items():
    for state_code, region in state.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]

# COMMAND ----------

df_region_mapping = spark.createDataFrame(rows)

df_region_mapping.display()

# COMMAND ----------

df_region_mapping = spark.createDataFrame(rows)

df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.table(f'{category_name}.silver.slv_customers')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region':'Other'})

display(df_gold.limit(50))

# COMMAND ----------

df_gold.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable(f'{category_name}.gold.gld_customers')

# COMMAND ----------

# MAGIC %md
# MAGIC #date and calender
# MAGIC

# COMMAND ----------

df_silver = spark.table(f'{category_name}.silver.slv_date')


# COMMAND ----------

df_gold = df_silver.withColumn('date_id',F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"),"MMMM"))

df_gold = df_gold.withColumn("is_weekend",F.when(F.col("day_name").isin("Saturday","Sunday"),1).otherwise(0))

display(df_gold.limit(5))

# COMMAND ----------

desired_columns_order = ["date_id","date","year","month_name","day_name","is_weekend","quarter","week","_source_file","ingested_at"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

df_gold.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable(f'{category_name}.gold.gld_date')


# COMMAND ----------

