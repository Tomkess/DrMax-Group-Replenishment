# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set up

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/_initSparkCluster

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/dataProcessor

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/removeBlobFiles

# COMMAND ----------

# MAGIC %run "/Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/model/ElasticNet__with_CV"

# COMMAND ----------

import platform

if platform.uname().node == 'GL22WD4W2DY33':
    from src._initSparkCluster import *

# Transfer File
tmp_blob_name = 'group_config_files/pl/config_pl.csv'
BLOCK_BLOB_SERVICE.create_blob_from_path(
    container_name='replenishment-modelling',
    blob_name=tmp_blob_name,
    file_path='C:/Users/peter.tomko/OneDrive - Dr. Max BDC, s.r.o/Plocha/ds_projects/DrMax-Group-Replenishment/data/init config pl replenishment/config_pl.csv')
