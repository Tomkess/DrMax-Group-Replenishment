# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set up

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/_initSparkCluster

# COMMAND ----------

import platform

if platform.uname().node == 'GL22WD4W2DY33':
    from src._initSparkCluster import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Loader

# COMMAND ----------


class DataProcessor:

    def __init__(self,
                 blob_name: str,
                 blob_loc: str,
                 header: bool,
                 sep: str,
                 infer_schema: bool,
                 encoding: str,
                 multi_line) -> None:

        self.blob_name = blob_name
        self.blob_loc = blob_loc
        self.header = header
        self.sep = sep
        self.infer_schema = infer_schema
        self.encoding = encoding
        self.multi_line = multi_line

    def read_csv_from_blob(self) -> pyspark.sql.DataFrame:
        """This function reads csv data from blob storage using pyspark.
        :return:
            DataFrame
        """

        # read csv data
        tmp_data = spark.read.csv(
            path='{}/{}'.format(CONT_PATH.format(self.blob_name, STORAGE_ACCOUNT), self.blob_loc),
            sep=self.sep,
            header=self.header,
            inferSchema=self.infer_schema,
            encoding=self.encoding,
            quote='"',
            multiLine=self.multi_line
        )

        # return data
        return tmp_data

    def process_data(self, sql_code: str):
        """
        This function processes data using sql code
        :param sql_code:
            str
        :return:
            pyspark.sql.DataFrame
        """
        tmp_data = self.read_csv_from_blob()
        tmp_data.createOrReplaceTempView('tmp_data')

        processed_data = spark.sql(sql_code)
        return processed_data
