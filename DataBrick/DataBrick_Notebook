# Databricks notebook source
# MAGIC %md
# MAGIC #Silver Layer script

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Access Using App

# COMMAND ----------



spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "<CLIENT_ID>"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "<CLIENT_SECRET>"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Calender data

# COMMAND ----------

df_cal = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Customers Data

# COMMAND ----------

df_cus = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Product Categories

# COMMAND ----------

df_procat = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Products

# COMMAND ----------

df_pro = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Returns

# COMMAND ----------

df_ret = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Sales 2015-2016-2017

# COMMAND ----------

df_sales = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Territories

# COMMAND ----------

df_ter = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Product_Subcategories

# COMMAND ----------

df_prosub = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@mystorageaccount123.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month',month(col("Date")))\
    .withColumn('Year',year(col("Date")))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn("FullName",concat_ws(" ",col("Prefix"),col("FirstName"),col("LastName")))
df_cus.display()

# COMMAND ----------

df_cus.write.format("parquet")\
    .mode("append")\
        .option( "path",f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/AdventureWorks_Customers" )\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Product Sub Category

# COMMAND ----------

df_prosub.display()

# COMMAND ----------

df_prosub.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn("ProductSKU",split(col("ProductSKU"),'-')[0])
df_pro = df_pro.withColumn("ProductName",split(col("ProductName"),' ')[0])


# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Returns")\
       .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Territories")\
       .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn("StockDate",to_timestamp("StockDate"))
df_sales = df_sales.withColumn("OrderNumber",regexp_replace("OrderNumber","S","T"))
df_sales = df_sales.withColumn("Multiply", col("OrderLineItem") * col("OrderQuantity"))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format("parquet")\
    .mode("append")\
        .option('path', 'abfss://silver@mystorageaccount123.dfs.core.windows.net/AdventureWorks_Sales')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Sales Analysis

# COMMAND ----------

df_sales.groupBy("OrderDate").agg(count("OrderNumber").alias("Total_Orders")).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

