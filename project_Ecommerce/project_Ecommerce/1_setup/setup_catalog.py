# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF not EXISTS ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists ecommerce.bronze;
# MAGIC create schema if not exists ecommerce.silver;
# MAGIC create schema if not exists ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases from ecommerce;

# COMMAND ----------

