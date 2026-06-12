# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to silver: Data Cleaning And Transformation Dimension Table

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_name", F.trim(F.col("brand_name")))
df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn("brand_code",F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]',''))

df_silver.show(10)

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------


anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}


df_silver = df_silver.replace(to_replace= anomalies, subset=["category_code"])

df_silver.show(10)

df_silver.select("category_code").distinct().show()

# COMMAND ----------



# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergerSchema", "true").saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC # category

# COMMAND ----------

df_bronze1 = spark.table(f"{catalog_name}.bronze.brz_categories")


df_bronze1.show(10)

# COMMAND ----------

df_duplicates = df_bronze1.groupBy("category_code").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze1.dropDuplicates(['category_code'])
display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)

# COMMAND ----------


df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true")\
        .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC # product

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products2")
 
row_count, column_count = df_bronze.count(), len(df_bronze.columns)
 
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

# COMMAND ----------

# check weight_gram column
df_bronze.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# replace 'g' with ' '
df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "" ).cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# check length_cm  (comma instead of dot)
 
df_silver.select("length_cm").show(3)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(3)

# COMMAND ----------

#covert category_code  and brand_code to upper case

df_silver = df_silver.withColumn("category_code",F.upper(F.col("category_code"))).withColumn("brand_code",F.upper(F.col("brand_code")))
#check if there are any null values in the dataframe
df_silver.select("category_code","brand_code").show(2)

# COMMAND ----------

df_silver.select("material").distinct().show()

# COMMAND ----------

anomalies = {
    "Coton": "Cotton",
    "Ruber": "Rubber",
    "Aluminum": "Aluminum"
}


df_silver = df_silver.replace(to_replace= anomalies, subset=["material"])

df_silver.select("material").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn("rating_count",F.when(F.col("rating_count").isNotNull(),F.abs(F.col("rating_count"))).otherwise(F.lit(0)))

df_silver.show(10)

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Customers
# MAGIC

# COMMAND ----------

#Read data
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

row_count, column_count = df_bronze.count(), len(df_bronze.columns)
print(f"Number of rows in bronze table: {row_count}")
print(f"Number of columns in bronze table: {column_count}")

# COMMAND ----------

#Handle null Values in customer_id column (300 count hai null value)
null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

#drop rows where customer_id is null
df_silver = df_bronze.dropna(subset=["customer_id"])
df_silver.count()


# COMMAND ----------

null_count = df_silver.filter(F.col("phone").isNull()).count()
null_count

# COMMAND ----------

df_silver = df_silver.fillna("Not Available", subset=["phone"])
df_silver.filter(F.col("phone").isNull()).show()

# COMMAND ----------

#write data into silver LAYER
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #Dates

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_dates")

row_count,column_count = df_bronze.count(), len(df_bronze.columns)
print(f"Number of rows in bronze table: {row_count}")
print(f"Number of columns in bronze table: {column_count}")

# COMMAND ----------

#Convert String to Date

from pyspark.sql.functions import to_date

df_silver = df_bronze.withColumn("date", to_date(df_bronze["date"],"dd-MM-yyyy"))
df_silver.printSchema()


# COMMAND ----------

#FInd Duplicates
df_duplicates = df_silver.groupBy("date").count().filter("count>1")
df_duplicates.show()
#drop duplicates
df_silver = df_silver.dropDuplicates(["date"])
df_silver.count()


# COMMAND ----------

#Capitalize firstLetter of each word in day_name
df_silver = df_silver.withColumn("day_name",F.initcap(F.col("day_name")))
df_silver.show(5)

# COMMAND ----------

#Convert negative week_year to positive


df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))

df_silver.show()




# COMMAND ----------

df_silver = df_silver.withColumn(
    "quarter",
    F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))
)

df_silver = df_silver.withColumn(
    "week_of_year",
    F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))
)

df_silver.show(3)

# COMMAND ----------


#Rename  a column
df_silver = df_silver.withColumnRenamed("week_of_year","week")


# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable(f"{catalog_name}.silver.slv_date")

# COMMAND ----------

