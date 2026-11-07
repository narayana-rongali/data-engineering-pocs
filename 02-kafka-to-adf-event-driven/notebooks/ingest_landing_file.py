# Databricks notebook source
# Runs from the ADF pipeline, once per landing file. Replaying a file is safe because it MERGEs on order_id.
from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("folder_path", "")
dbutils.widgets.text("file_name", "")

container, _, folder = dbutils.widgets.get("folder_path").partition("/")
account = dbutils.widgets.get("storage_account")
source = f"abfss://{container}@{account}.dfs.core.windows.net/{folder}/{dbutils.widgets.get('file_name')}"
TARGET = "main.bronze.orders"

# COMMAND ----------
incoming = spark.read.json(source).withColumn("_source_file", F.lit(source)).dropDuplicates(["order_id"])

if spark.catalog.tableExists(TARGET):
    (
        DeltaTable.forName(spark, TARGET)
        .alias("t")
        .merge(incoming.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    incoming.write.saveAsTable(TARGET)
