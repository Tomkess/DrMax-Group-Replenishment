# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Function

# COMMAND ----------


def list_blob_files(blob_loc, cont_nm, block_blob_service):
    names = []
    next_marker = None
    while True:
        blob_list = block_blob_service.list_blobs(cont_nm, prefix=blob_loc + '/', num_results=5000, marker=next_marker)
        for blob in blob_list:
            names.append(blob.name)

        next_marker = blob_list.next_marker
        if not next_marker:
            break

    return names


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Remove files from Blob

# COMMAND ----------

def rem_blob_files(blob_loc, cont_nm, block_blob_service):
    # remove files
    names = list_blob_files(blob_loc=blob_loc, cont_nm=cont_nm, block_blob_service=block_blob_service)
    for i in names:
        block_blob_service.delete_blob(cont_nm, i)

    # remove master folder
    master_blob_loc = blob_loc.replace('/' + blob_loc.split('/')[-1], '')
    names = list_blob_files(blob_loc=master_blob_loc, cont_nm=cont_nm, block_blob_service=block_blob_service)
    for i in names:
        if blob_loc == i:
            block_blob_service.delete_blob(cont_nm, i)
