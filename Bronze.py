# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.healthcareproject12.dfs.core.windows.net",
  "ym06SBsQIBn/EAc8VPR5ko550FeO0veax+l9s1V25piCu3AiFCVC+3VDEOSdHqhD0CRzQkypuN5B+AStV/tkHQ=="
)

# COMMAND ----------

dbutils.fs.ls("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/")

# COMMAND ----------

df = spark.read.option("header", True).csv(
  "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/healthcare-raw"
)

display(df)

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/healthcare-raw")

df.display()

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/bronze/healthcare_bronze")

# COMMAND ----------

df = spark.read.format("delta").load(
  "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/bronze/healthcare_bronze"
)

df.printSchema()
df.count()
df.select("No-show").groupBy("No-show").count().show()

# COMMAND ----------

