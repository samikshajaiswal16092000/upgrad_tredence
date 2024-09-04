# Databricks notebook source
# MAGIC %run /Workspace/Users/samiksha_1724572427295@npupgradassessment.onmicrosoft.com/day10/day8/includes

# COMMAND ----------

dbutils.widgets.text("environment","dev")
v=dbutils.widgets.get("environment")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df2=df1.withColumn("environment",lit(v))
df2.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("bronze.products_bronze")
