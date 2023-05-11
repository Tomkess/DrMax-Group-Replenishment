# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set Up

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/_initSparkCluster

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/dataProcessor

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/dataProcessor

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/removeBlobFiles

# COMMAND ----------

# MAGIC %run "/Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/model/elastic net with cv"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Running Replenishment Model

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Poland

# COMMAND ----------

# MAGIC %run "/Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/bin/modelling_pipeline/pl_modelling_data"

# COMMAND ----------

# MAGIC %run "/Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/bin/modelling_pipeline/pl___elastic_net_estimation"

# COMMAND ----------
