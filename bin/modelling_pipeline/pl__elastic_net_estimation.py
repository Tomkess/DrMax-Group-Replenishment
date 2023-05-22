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

# MAGIC %run "/Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/model/ElasticNet_with_CV"

# COMMAND ----------

import platform

if platform.uname().node == 'GL22WD4W2DY33':
    from src._initSparkCluster import *
    from src.removeBlobFiles import *
    from model.ElasticNet__with_CV import *

    proj_path = 'C:/Users/peter.tomko/OneDrive - Dr. Max BDC, s.r.o/Plocha/ds_projects/DrMax-Group-Replenishment'
    local_path = '/data/tmp_data.csv'
else:
    proj_path = ''
    local_path = '/dbfs/data/tmp_data.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Finalize Features

# COMMAND ----------
final_cat_features = [
    # 'brand',
    # 'first_category',
    'me_brand_purchase_most_freq',
    'me_browser_most_freq',
    'me_city',
    # 'me_city_last',
    # 'me_country',
    'me_delivery_most_freq',
    'me_device_brand_most_freq',
    'me_device_brand_most_freq_second',
    'me_device_model_most_freq',
    'me_device_model_most_freq_second',
    'me_device_type_most_freq',
    'me_device_type_most_freq_second',
    'mx_gender_est_last',
    'me_os_most_freq',
    'me_payment_most_freq'
    # 'me_product_purchase_most_freq',
    # 'me_product_viewed_most_freq',
    # 'second_category',
    # 'third_category',
]

tmp_final_num_features = [
    # 'n_identifiers',
    # 'hours_since__first_event',
    # 'hours_since__modified',
    # 'hours_since__calculated',
    # 'customer_events_count',
    'se_click_rate',
    'se_email_clicked_count',
    'hours_since__se_email_clicked_date_last',
    'se_email_read_count',
    'hours_since__se_email_read_date_last',
    'se_email_sent_count',
    'hours_since__se_email_sent_date_last',
    'se_engagement',
    'se_events_count',
    'se_open_rate',
    'me_events_count',
    'me_visits_count',
    'me_page_views_avg',
    'me_amount_spent_all',
    'me_amount_spent_avg',
    # 'hours_since__me_transaction_datetime_first',
    'hours_since__me_transaction_datetime_last',
    'me_transaction_count',
    'me_engagement',
    'me_spend_percentile_quintile',
    'mx_email_count_unique',
    'web_time_spent_sum',
    'hours_since__me_registration_date',
    'magento',
    'me_web',
    'se',
    'purchased_quantity_last1m',
    'purchased_quantity_last3m',
    'purchased_quantity_last6m',
    'purchased_quantity_last12m',
    'banner_10__last12m_clicked_banner',
    'banner_10__last1m_clicked_banner',
    'banner_10__last3m_clicked_banner',
    'banner_10__last6m_clicked_banner',
    'banner_1__last12m_clicked_banner',
    'banner_1__last1m_clicked_banner',
    'banner_1__last3m_clicked_banner',
    'banner_1__last6m_clicked_banner',
    'banner_2__last12m_clicked_banner',
    'banner_2__last1m_clicked_banner',
    'banner_2__last3m_clicked_banner',
    'banner_2__last6m_clicked_banner',
    'banner_3__last12m_clicked_banner',
    'banner_3__last1m_clicked_banner',
    'banner_3__last3m_clicked_banner',
    'banner_3__last6m_clicked_banner',
    'banner_4__last12m_clicked_banner',
    'banner_4__last1m_clicked_banner',
    'banner_4__last3m_clicked_banner',
    'banner_4__last6m_clicked_banner',
    'banner_5__last12m_clicked_banner',
    'banner_5__last1m_clicked_banner',
    'banner_5__last3m_clicked_banner',
    'banner_5__last6m_clicked_banner',
    'banner_6__last12m_clicked_banner',
    'banner_6__last1m_clicked_banner',
    'banner_6__last3m_clicked_banner',
    'banner_6__last6m_clicked_banner',
    'banner_7__last12m_clicked_banner',
    'banner_7__last1m_clicked_banner',
    'banner_7__last3m_clicked_banner',
    'banner_7__last6m_clicked_banner',
    'banner_8__last12m_clicked_banner',
    'banner_8__last1m_clicked_banner',
    'banner_8__last3m_clicked_banner',
    'banner_8__last6m_clicked_banner',
    'banner_9__last12m_clicked_banner',
    'banner_9__last1m_clicked_banner',
    'banner_9__last3m_clicked_banner',
    'banner_9__last6m_clicked_banner',
    'banner_10__last12m_impression_banner',
    'banner_10__last1m_impression_banner',
    'banner_10__last3m_impression_banner',
    'banner_10__last6m_impression_banner',
    'banner_1__last12m_impression_banner',
    'banner_1__last1m_impression_banner',
    'banner_1__last3m_impression_banner',
    'banner_1__last6m_impression_banner',
    'banner_2__last12m_impression_banner',
    'banner_2__last1m_impression_banner',
    'banner_2__last3m_impression_banner',
    'banner_2__last6m_impression_banner',
    'banner_3__last12m_impression_banner',
    'banner_3__last1m_impression_banner',
    'banner_3__last3m_impression_banner',
    'banner_3__last6m_impression_banner',
    'banner_4__last12m_impression_banner',
    'banner_4__last1m_impression_banner',
    'banner_4__last3m_impression_banner',
    'banner_4__last6m_impression_banner',
    'banner_5__last12m_impression_banner',
    'banner_5__last1m_impression_banner',
    'banner_5__last3m_impression_banner',
    'banner_5__last6m_impression_banner',
    'banner_6__last12m_impression_banner',
    'banner_6__last1m_impression_banner',
    'banner_6__last3m_impression_banner',
    'banner_6__last6m_impression_banner',
    'banner_7__last12m_impression_banner',
    'banner_7__last1m_impression_banner',
    'banner_7__last3m_impression_banner',
    'banner_7__last6m_impression_banner',
    'banner_8__last12m_impression_banner',
    'banner_8__last1m_impression_banner',
    'banner_8__last3m_impression_banner',
    'banner_8__last6m_impression_banner',
    'banner_9__last12m_impression_banner',
    'banner_9__last1m_impression_banner',
    'banner_9__last3m_impression_banner',
    'banner_9__last6m_impression_banner',
    'last1m_product_view',
    'last3m_product_view',
    'last6m_product_view',
    'last12m_product_view',
    'campaign_10__last12m_utm_campaign',
    'campaign_10__last1m_utm_campaign',
    'campaign_10__last3m_utm_campaign',
    'campaign_10__last6m_utm_campaign',
    'campaign_1__last12m_utm_campaign',
    'campaign_1__last1m_utm_campaign',
    'campaign_1__last3m_utm_campaign',
    'campaign_1__last6m_utm_campaign',
    'campaign_2__last12m_utm_campaign',
    'campaign_2__last1m_utm_campaign',
    'campaign_2__last3m_utm_campaign',
    'campaign_2__last6m_utm_campaign',
    'campaign_3__last12m_utm_campaign',
    'campaign_3__last1m_utm_campaign',
    'campaign_3__last3m_utm_campaign',
    'campaign_3__last6m_utm_campaign',
    'campaign_4__last12m_utm_campaign',
    'campaign_4__last1m_utm_campaign',
    'campaign_4__last3m_utm_campaign',
    'campaign_4__last6m_utm_campaign',
    'campaign_5__last12m_utm_campaign',
    'campaign_5__last1m_utm_campaign',
    'campaign_5__last3m_utm_campaign',
    'campaign_5__last6m_utm_campaign',
    'campaign_6__last12m_utm_campaign',
    'campaign_6__last1m_utm_campaign',
    'campaign_6__last3m_utm_campaign',
    'campaign_6__last6m_utm_campaign',
    'campaign_7__last12m_utm_campaign',
    'campaign_7__last1m_utm_campaign',
    'campaign_7__last3m_utm_campaign',
    'campaign_7__last6m_utm_campaign',
    'campaign_8__last12m_utm_campaign',
    'campaign_8__last1m_utm_campaign',
    'campaign_8__last3m_utm_campaign',
    'campaign_8__last6m_utm_campaign',
    'campaign_9__last12m_utm_campaign',
    'campaign_9__last1m_utm_campaign',
    'campaign_9__last3m_utm_campaign',
    'campaign_9__last6m_utm_campaign',
    'medium_10__last12m_utm_medium',
    'medium_10__last1m_utm_medium',
    'medium_10__last3m_utm_medium',
    'medium_10__last6m_utm_medium',
    'medium_1__last12m_utm_medium',
    'medium_1__last1m_utm_medium',
    'medium_1__last3m_utm_medium',
    'medium_1__last6m_utm_medium',
    'medium_2__last12m_utm_medium',
    'medium_2__last1m_utm_medium',
    'medium_2__last3m_utm_medium',
    'medium_2__last6m_utm_medium',
    'medium_3__last12m_utm_medium',
    'medium_3__last1m_utm_medium',
    'medium_3__last3m_utm_medium',
    'medium_3__last6m_utm_medium',
    'medium_4__last12m_utm_medium',
    'medium_4__last1m_utm_medium',
    'medium_4__last3m_utm_medium',
    'medium_4__last6m_utm_medium',
    'medium_5__last12m_utm_medium',
    'medium_5__last1m_utm_medium',
    'medium_5__last3m_utm_medium',
    'medium_5__last6m_utm_medium',
    'medium_6__last12m_utm_medium',
    'medium_6__last1m_utm_medium',
    'medium_6__last3m_utm_medium',
    'medium_6__last6m_utm_medium',
    'medium_7__last12m_utm_medium',
    'medium_7__last1m_utm_medium',
    'medium_7__last3m_utm_medium',
    'medium_7__last6m_utm_medium',
    'medium_8__last12m_utm_medium',
    'medium_8__last1m_utm_medium',
    'medium_8__last3m_utm_medium',
    'medium_8__last6m_utm_medium',
    'medium_9__last12m_utm_medium',
    'medium_9__last1m_utm_medium',
    'medium_9__last3m_utm_medium',
    'medium_9__last6m_utm_medium',
    'source_1__last12m_utm_source',
    'source_1__last1m_utm_source',
    'source_1__last3m_utm_source',
    'source_1__last6m_utm_source',
    'source_2__last12m_utm_source',
    'source_2__last1m_utm_source',
    'source_2__last3m_utm_source',
    'source_2__last6m_utm_source',
    'source_3__last12m_utm_source',
    'source_3__last1m_utm_source',
    'source_3__last3m_utm_source',
    'source_3__last6m_utm_source',
    'source_4__last12m_utm_source',
    'source_4__last1m_utm_source',
    'source_4__last3m_utm_source',
    'source_4__last6m_utm_source',
    'source_5__last12m_utm_source',
    'source_5__last1m_utm_source',
    'source_5__last3m_utm_source',
    'source_5__last6m_utm_source'
]
final_num_features = [x for x in tmp_final_num_features
                      if 'last1m' not in x and 'last6m' not in x and 'last12m' not in x
                      and 'utm_source' not in x and 'utm_medium' not in x and 'utm_campaign' not in x
                      and 'impression_banner' not in x and 'clicked_banner' not in x]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Estimate Model

# COMMAND ----------

# MinMaxScaler is only supported!
elnet_model = ElasticNetCV(
    storage_account=STORAGE_ACCOUNT,
    blob_name='replenishment-modelling',
    blob_loc='1_model_df/group_replenishment_modelling/pl_modelling_data',
    cat_features=final_cat_features,
    num_features=final_num_features,
    label='log_days_since_last_purchase',
    metric_name='rmse',
    scaler_name='MinMaxScaler',
    alpha=[0, 0.001, 0.01, 0.1, 0.2, 0.75, 1],
    l1_ratio=[0.001, 0.01, 0.1, 0.2, 0.75, 1],
    top_features=150,
    log_transform=True,
    cv=25,
    seed=42)

cvModel, master_data, master_feature_db = elnet_model.fit()
print('Finished Training!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to AZURE

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write Coefficients

# COMMAND ----------

current_dt = str(datetime.date(datetime.now())).replace('-', '')
blob_path = '5_model_storage/pl/meiro_estimation/coefficients/elastic_net__' + current_dt + '.csv'

master_feature_db.to_csv(proj_path + local_path, sep='~', index=False)
BLOCK_BLOB_SERVICE.create_blob_from_path(container_name='replenishment-modelling',
                                         blob_name=blob_path,
                                         file_path=proj_path+local_path)
print('Coefficients Written!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write cvModel

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Historization

# COMMAND ----------

blob_path = '5_model_storage/pl/meiro_estimation/cv_model/elastic_net__' + current_dt
WPATH = '{}/{}'.format(CONT_PATH.format('replenishment-modelling', STORAGE_ACCOUNT), blob_path)
rem_blob_files(blob_loc=blob_path, cont_nm='replenishment-modelling', block_blob_service=BLOCK_BLOB_SERVICE)

cvModel.save(WPATH)
print('cvModel Written - Historization!')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write Master Data as Parquet

# COMMAND ----------
blob_path = '5_model_storage/pl/meiro_estimation/master_data'
WPATH = '{}/{}'.format(CONT_PATH.format('replenishment-modelling', STORAGE_ACCOUNT), blob_path)
rem_blob_files(blob_loc=blob_path, cont_nm='replenishment-modelling', block_blob_service=BLOCK_BLOB_SERVICE)

(master_data
 .write
 .mode('overwrite')
 .option('header', 'true')
 .format('parquet')
 .save(WPATH))
print('Master Data Written!')
