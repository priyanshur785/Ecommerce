# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze to Silver: data cleaning and transformation

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *


# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df = spark.table(f"{catalog_name}.bronze.brz_order_items")

df.show(5)

# COMMAND ----------

df = df.dropDuplicates(['order_id','item_seq'])
 
df = df.withColumn(
    'quantity', when(col('quantity')=='Two',2).otherwise(col('quantity')).cast('int')
)
 
df = df.withColumn(
    'unit_price', regexp_replace('unit_price','[$]','').cast('double')
)
 
df = df.withColumn(
    'discount_pct', regexp_replace('discount_pct','%','').cast('double')
)
 
df= df.withColumn(
    'coupon_code',lower(trim(col('coupon_code')))
)
 
df = df.withColumn(
    'channel',when(col('channel')=='web','Website')
    .when(col('channel')=='app','Mobile')
    .otherwise(col('channel'))
)

# COMMAND ----------

# DBTITLE 1,Cell 6

df = df.withColumn("dt", to_date("dt", "yyyy-MM-dd"))

df = df.withColumn("order_ts", trim(col("order_ts")))

df = df.withColumn("order_ts",
    coalesce(
        to_timestamp(col("order_ts"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("order_ts"), "dd-MM-yyyy HH:mm:ss")
    )
)


df = df.withColumn("item_seq", col("item_seq").cast("int"))

df = df.withColumn("tax_amount",
    regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
)

df = df.withColumn("processed_time", current_timestamp())



# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.silver.slv_order_items")    

# COMMAND ----------

