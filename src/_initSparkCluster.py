# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Import Libraries

# COMMAND ----------

import os
import platform

import pyspark
from azure.storage.blob import BlockBlobService

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Blob Storage Backend

# COMMAND ----------

STORAGE_ACCOUNT = os.getenv('dsBlobStorage')
ACCESS_KEY = os.getenv('dsBlobToken')
CONT_PATH = 'wasbs://{}@{}.blob.core.windows.net'
ACCOUNT_KEY = 'fs.azure.account.key.{}.blob.core.windows.net'.format(STORAGE_ACCOUNT)
BLOCK_BLOB_SERVICE = BlockBlobService(account_name=STORAGE_ACCOUNT, account_key=ACCESS_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Spark Cluster

# COMMAND ----------

if platform.uname().node == 'GL22WD4W2DY33':
    spark = (pyspark
             .sql
             .SparkSession
             .builder
             .appName('General')
             .config('spark.driver.maxResultSize', '16g')
             .config('spark.executor.memory', '16g')
             .config('spark.driver.memory', '16g')
             .config('spark.executor.cores', 1)
             .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 64)
             .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')
             .getOrCreate())

spark.conf.set(ACCOUNT_KEY, ACCESS_KEY)
spark._jsc.hadoopConfiguration().set(ACCOUNT_KEY, ACCESS_KEY)
