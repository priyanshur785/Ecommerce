# Databricks notebook source
import pyspark.sql. functions as f
from pyspark.sql.types import *
catalog_name = "ecommerce"

# COMMAND ----------

df = spark.table(f"{catalog_name}.silver.slv_order_items")
df.limit(10).display()


# COMMAND ----------

# 1) Add gross amount
df = df.withColumn('gross_amount', col('unit_price') * col('quantity'))
 
# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df = df.withColumn('discount_amount', ceil(col('gross_amount') * col('discount_pct') / 100))
 
# 3) Add sale_amount (gross_amount - discount_amount)
df = df.withColumn("sale_amount", col("gross_amount") - col("discount_amount") + col("tax_amount"))
 
# add date id
df = df .withColumn('date_id', date_format(col('dt'),"yyyyMMdd").cast(IntegerType()))
 
#coupan flag
#coupan flag = 1 if coupan_code is not null else 0
df = df.withColumn('coupan_flag', when(col('coupon_code').isNotNull(), lit(1)).otherwise(lit(0)))
 
df.limit(5).display()

# COMMAND ----------

fix_rates = {
    "INR" : 1.00,
    "AED" : 24.18,
    "AUD" : 57.55,
    "CAD" : 62.93,
    "GBP" : 117.98,
    "SGD" : 68.18,
    "USD" : 88.29
}

rates = [(k, float(v)) for k, v in fix_rates.items()]
rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()

# COMMAND ----------

df = df.withColumn("sale_amount", col("gross_amount") - col("discount_amount") + col("tax_amount"))

df = df.join(rates_df, rates_df.currency == upper(trim(col("unit_price_currency"))), "left")

df = df.withColumn("sale_amount_inr", col("sale_amount") * col("inr_rate"))
df = df.withColumn("sale_amount_inr", ceil(col("sale_amount_inr")))
 
df.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Cell 6
orders_gold_df =  df.select(
    col("date_id"),
    col("dt").alias("transaction_date"),
    col("order_ts").alias("transaction_ts"),
    col("order_id").alias("transaction_id"),
    f.col("customer_id"),
    f.col("item_seq").alias("seq_no"),
    f.col("product_id"),
    f.col("channel"),
    f.col("coupon_code"),
    f.col("coupan_flag"),
    f.col("unit_price_currency"),
    f.col("quantity"),
    f.col("unit_price"),
    f.col("gross_amount"),
    f.col("discount_pct").alias("discout_percent"),
    f.col("discount_amount"),
    f.col("tax_amount"),
    f.col("sale_amount").alias("net_amount"),
    f.col("sale_amount_inr").alias("net_amount_inr")
)

# COMMAND ----------

orders_gold_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.gold.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view ecommerce.gold.fact_transactions as (
# MAGIC     select i.* , c.year , c.month_name ,
# MAGIC     c.day_name , c.is_weekend , c.quarter , c.week ,
# MAGIC     p.sku , p.category_code ,
# MAGIC     p.brand_code , p.brand_name , p.color , p.size , p.rating_count ,
# MAGIC     extract(HOUR from transaction_ts) as hour_of_day from ecommerce.gold.orders i
# MAGIC     join ecommerce.gold.gld_date c on i.date_id = c.date_id
# MAGIC     join ecommerce.gold.gold_dim_products p on i.product_id = p.product_id);

# COMMAND ----------

df = spark.read.table("ecommerce.gold.fact_transactions")
df.printSchema()

# COMMAND ----------

