# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Dimention data into bronze
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name ='ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC # Brands

# COMMAND ----------

brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name",StringType(), True),
    StructField("category_code",StringType(), True),
])

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/brands.csv"

df = spark.read.option('header','true').option('delimiter',',').schema(brand_schema).csv(raw_data_path)
df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at",F.current_timestamp())




# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC # Category

# COMMAND ----------

category_schema = StructType([
    StructField("category_code",StringType(),False),
    StructField("category_name",StringType(),True)
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/category/category.csv"

df = spark.read.option("header","true").option("delimiter",",").schema(category_schema).csv(raw_data_path)

df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at",F.current_timestamp())


df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.bronze.brz_categories")

# COMMAND ----------

# MAGIC %md
# MAGIC # products

# COMMAND ----------

products_schema = StructType([
    StructField("product_id",StringType(),False),
    StructField("sku",StringType(),True),
    StructField("category_code",StringType(),True),
    StructField("brand_code",StringType(),True),
    StructField("color",StringType(),True),
    StructField("size",StringType(),True),
    StructField("material",StringType(),True),
    StructField("weight_grams",StringType(),True),
    StructField("length_cm", StringType(),True),
    StructField("width_cm", FloatType(),True),
    StructField("height_cm", FloatType(),True),
    StructField("rating_count", FloatType(),True),
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/products/products.csv"

df = spark.read.option("header","true").option("delimiter",",").schema(products_schema).csv(raw_data_path)

df = df.withColumn("_source_file",F.col("_metadata.file_path")).withColumn("ingested_at",F.current_timestamp())


df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.bronze.brz_products2")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.bronze.brz_products1")

# COMMAND ----------

# MAGIC %md
# MAGIC # customer

# COMMAND ----------

customer_schema = StructType([
    StructField('customer_id',StringType(),False),
    StructField('phone',StringType(),True),
    StructField('country_code',StringType(),True),
    StructField('country',StringType(),True),
    StructField('state',StringType(),True)
])
 
raw_path = '/Volumes/ecommerce/source_data/raw/customers/customers.csv'
 
df_cust = spark.read.option('header','true').option('delimeter',',').schema(customer_schema).csv(raw_path)
 
df_cust = df_cust.withColumn('_source_file',F.col("_metadata.file_path")).withColumn('ingested_at',F.current_timestamp())
 
display(df_cust.limit(5))
df_cust.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable(f'{catalog_name}.bronze.brz_customers')

# COMMAND ----------

# MAGIC %md
# MAGIC # date

# COMMAND ----------

date_schema = StructType([
    StructField('date',StringType(),False),
    StructField('year',IntegerType(),True),
    StructField('day_name',StringType(),True),
    StructField('quarter',IntegerType(),True),
    StructField('week_of_year',IntegerType(),True)
])
 
raw_path = '/Volumes/ecommerce/source_data/raw/date/date.csv'
 
df_date = spark.read.option('header','true').option('delimeter',',').schema(date_schema).csv(raw_path)
 
df_date = df_date.withColumn('_source_file',F.col("_metadata.file_path")).withColumn('ingested_at',F.current_timestamp())
 
display(df_date.limit(5))
df_date.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable(f'{catalog_name}.bronze.brz_dates')
 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC

# COMMAND ----------

