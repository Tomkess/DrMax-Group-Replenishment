# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Set up

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/_initSparkCluster

# COMMAND ----------

# MAGIC %run /Repos/adm.test.tomko@drmaxtest.onmicrosoft.com/DrMax-Group-Replenishment/src/dataProcessor

# COMMAND ----------

import platform
from pyspark.sql import functions as F

if platform.uname().node == 'GL22WD4W2DY33':
    from src._initSparkCluster import *
    from src.dataProcessor import DataProcessor
    from src.removeBlobFiles import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Loader

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Replenishment Target

# COMMAND ----------

replenishmentTarget = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/replenishment_target.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
replenishment_target = replenishmentTarget.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        product_id,
        to_timestamp(substring_index(purchase_dt, '.', 1)) as purchase_dt,
        to_timestamp(substring_index(last_purchase_dt, '.', 1)) as last_purchase_dt,
        
        (unix_timestamp(substring_index(purchase_dt, '.', 1)) - unix_timestamp(substring_index(last_purchase_dt, '.', 1)))/(60 * 60 * 24) as days_since_last_purchase
    FROM tmp_data
""")
# replenishment_target.show()
replenishment_target.createOrReplaceTempView('replenishment_target')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Identity Identifiers

# COMMAND ----------

identityIdentifiers = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/identity_identifiers.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
identity_identifiers = identityIdentifiers.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        cast(n_identifiers as double) as n_identifiers
    FROM tmp_data
""")
identity_identifiers.createOrReplaceTempView('identity_identifiers')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Identity Sources

# COMMAND ----------

identitySources = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/identity_sources.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
identity_sources = identitySources.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        sources,
        1 as in_source
    FROM tmp_data
    where customer_entity_id is not null
""").groupBy('customer_entity_id').pivot('sources').agg(F.sum('in_source').alias('in_source')).na.fill(0)
# identity_sources.show()
identity_sources.createOrReplaceTempView('identity_sources')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Transactional Features

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Product Quantity Bought

# COMMAND ----------

transactionalQuantity = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/trx_features/transaction_product_quantity.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
transactional_quantity = transactionalQuantity.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        product_id,
        to_timestamp(substring_index(purchase_dt, '.', 1)) as purchase_dt,
        cast(quantity as double) as quantity
    FROM tmp_data
""")
# transactional_quantity.show()
transactional_quantity.createOrReplaceTempView('transactional_quantity')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Transaction Attributes

# COMMAND ----------

transactionalAttributes = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/trx_features/transaction_attributes.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
transactional_attributes = transactionalAttributes.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(purchase_dt, '.', 1)) as purchase_dt,
        transaction_id,
        delivery_type,
        payment_type
    FROM tmp_data
""")
# transactional_attributes.show()
transactional_attributes.createOrReplaceTempView('transactional_attributes')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Coupon Used

# COMMAND ----------

couponUsed = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/trx_features/coupon_used.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
coupon_used = couponUsed.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        transaction_id,
        voucher_id,
        to_timestamp(substring_index(coupon_dt, '.', 1)) AS coupon_dt
    FROM tmp_data
""")
# coupon_used.show()
coupon_used.createOrReplaceTempView('coupon_used')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Products

# COMMAND ----------

products = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/product_features/product_feed.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
products = products.process_data(sql_code="""
    SELECT 
        item_id,
        IFNULL(trim(split(product_category, '>')[0]), 'Unknown') as first_category,
        IFNULL(trim(split(product_category, '>')[1]), 'Unknown') as second_category,
        IFNULL(trim(split(product_category, '>')[2]), 'Unknown') as third_category,
        brand,
        cast(price AS DOUBLE) AS price
    FROM tmp_data
""")
# products.show()
products.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Identity Attributes

# COMMAND ----------

identityAttributes = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/identity_attributes/*.csv',
    header=True,
    sep='~',
    infer_schema=False,
    encoding='utf-8'
)
identity_attributes = identityAttributes.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        cast(hours_since__first_event AS DOUBLE) AS hours_since__first_event,
        cast(hours_since__modified AS DOUBLE) AS hours_since__modified,
        cast(hours_since__calculated AS DOUBLE) AS hours_since__calculated,
        cast(customer_events_count AS DOUBLE) AS customer_events_count,
        cast(se_click_rate AS DOUBLE) AS se_click_rate,
        cast(se_email_clicked_count AS DOUBLE) AS se_email_clicked_count,
        cast(hours_since__se_email_clicked_date_last AS DOUBLE) AS hours_since__se_email_clicked_date_last,
        cast(se_email_read_count AS DOUBLE) AS se_email_read_count,
        cast(hours_since__se_email_read_date_last AS DOUBLE) AS hours_since__se_email_read_date_last,
        cast(se_email_sent_count AS DOUBLE) AS se_email_sent_count,
        cast(hours_since__se_email_sent_date_last AS DOUBLE) AS hours_since__se_email_sent_date_last,
        cast(se_engagement AS DOUBLE) AS se_engagement,
        cast(se_events_count AS DOUBLE) AS se_events_count,
        cast(se_open_rate AS DOUBLE) AS se_open_rate,
        cast(me_events_count AS DOUBLE) AS me_events_count,
        cast(me_visits_count AS DOUBLE) AS me_visits_count,
        cast(me_os_most_freq AS DOUBLE) AS me_os_most_freq,
        cast(me_page_views_avg AS DOUBLE) AS me_page_views_avg,
        cast(me_amount_spent_all AS DOUBLE) AS me_amount_spent_all,
        ifnull(me_browser_most_freq, 'Unknown') AS me_browser_most_freq,
        ifnull(me_device_type_most_freq, 'Unknown') AS me_device_type_most_freq,
        ifnull(me_device_model_most_freq, 'Unknown') AS me_device_model_most_freq,
        ifnull(me_device_brand_most_freq, 'Unknown') AS me_device_brand_most_freq,
        ifnull(me_country, 'Unknown') AS me_country,
        ifnull(me_city, 'Unknown') AS me_city,
        ifnull(me_city_last, 'Unknown') AS me_city_last,
        ifnull(me_payment_most_freq, 'Unknown') AS me_payment_most_freq,
        ifnull(me_delivery_most_freq, 'Unknown') AS me_delivery_most_freq,
        ifnull(me_device_model_most_freq_second, 'Unknown') AS me_device_model_most_freq_second,
        ifnull(me_device_brand_most_freq_second, 'Unknown') AS me_device_brand_most_freq_second,
        ifnull(me_device_type_most_freq_second, 'Unknown') AS me_device_type_most_freq_second,
        cast(me_amount_spent_avg AS DOUBLE) AS me_amount_spent_avg,
        cast(hours_since__me_transaction_datetime_first AS DOUBLE) AS hours_since__me_transaction_datetime_first,
        cast(hours_since__me_transaction_datetime_last AS DOUBLE) AS hours_since__me_transaction_datetime_last,
        ifnull(me_brand_purchase_most_freq, 'Unknown') AS me_brand_purchase_most_freq,
        ifnull(me_product_purchase_most_freq, 'Unknown') AS me_product_purchase_most_freq,
        ifnull(me_product_viewed_most_freq, 'Unknown') AS me_product_viewed_most_freq,
        ifnull(cast(me_transaction_count AS DOUBLE), 0) AS me_transaction_count,
        cast(me_engagement AS DOUBLE) AS me_engagement,
        cast(me_spend_percentile_quintile AS DOUBLE) AS me_spend_percentile_quintile,
        ifnull(mx_gender_est_last, 'Unknown') AS mx_gender_est_last,
        cast(mx_email_count_unique AS DOUBLE) AS mx_email_count_unique,
        cast(web_time_spent_sum AS DOUBLE) AS web_time_spent_sum,
        CAST(hours_since__me_registration_date AS DOUBLE) AS hours_since__me_registration_date
    FROM tmp_data
""")
# identity_attributes.show()
identity_attributes.createOrReplaceTempView('identity_attributes')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SmartEmailing Features

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Received Campaigns

# COMMAND ----------

receivedCampaigns = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/smartemailing_features/campaigns_received.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)

received_campaigns = receivedCampaigns.process_data(sql_code="""
SELECT 
    customer_entity_id,
    to_timestamp(substring_index(received_dt, '.', 1)) as opened_dt,
    automation_name,
    automation_id
FROM tmp_data
WHERE automation_name is not null
""")
# received_campaigns.show()
received_campaigns.createOrReplaceTempView('received_campaigns')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Opened Campaigns

# COMMAND ----------

openedCampaigns = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/smartemailing_features/campaigns_opened.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
opened_campaigns = openedCampaigns.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(opened_dt, '.', 1)) as opened_dt,
        automation_name,
        automation_id
    FROM tmp_data
    WHERE automation_name is not null
""")
# opened_campaigns.show()
opened_campaigns.createOrReplaceTempView('opened_campaigns')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Clicked Campaigns

# COMMAND ----------

clickedCampaigns = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/smartemailing_features/campaigns_clicked.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
clicked_campaigns = clickedCampaigns.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(clicked_dt, '.', 1)) as clicked_dt,
        automation_name,
        automation_id
    FROM tmp_data
    WHERE automation_name is not null
""")
# clicked_campaigns.show()
clicked_campaigns.createOrReplaceTempView('clicked_campaigns')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### UTM Features

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### UTM Source

# COMMAND ----------

utmSource = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/utm_features/utm_source.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
utm_source = utmSource.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(source_dt, '.', 1)) as source_dt,
        utm_source
    FROM tmp_data
""")
# utm_source.show()
utm_source.createOrReplaceTempView('utm_source')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### UTM Medium

# COMMAND ----------

utmMedium = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/utm_features/utm_medium.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
utm_medium = utmMedium.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(medium_dt, '.', 1)) as medium_dt,
        utm_medium
    FROM tmp_data
""")
# utm_medium.show()
utm_medium.createOrReplaceTempView('utm_medium')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### UTM Medium

# COMMAND ----------

utmCampaign = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/utm_features/utm_campaign.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
utm_campaign = utmCampaign.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(campaign_dt, '.', 1)) as campaign_dt,
        utm_campaign
    FROM tmp_data
""")
# utm_campaign.show()
utm_campaign.createOrReplaceTempView('utm_campaign')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Web Features

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Product Views

# COMMAND ----------

productViews = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/web_features/products_viewed.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
product_views = productViews.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(viewed_dt, '.', 1)) as viewed_dt,
        product_id    
    FROM tmp_data
""")
# product_views.show()
product_views.createOrReplaceTempView('product_views')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Web banner clicked

# COMMAND ----------

bannerClicked = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/web_features/web_banner_clicked.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
banner_clicked = bannerClicked.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(clicked_dt, '.', 1)) as clicked_dt,
        banner_name,
        banner_id
    FROM tmp_data
""")
# banner_clicked.show()
banner_clicked.createOrReplaceTempView('banner_clicked')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Web banner impression

# COMMAND ----------

bannerImpression = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/web_features/web_banner_impression.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
banner_impression = bannerImpression.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring(impression_dt, '.', 1)) as impression_dt,
        banner_name,
        banner_id
    FROM tmp_data
""")
# banner_impression.show()
banner_impression.createOrReplaceTempView('banner_impression')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Web searches

# COMMAND ----------

webSearches = DataProcessor(
    blob_name='replenishment-modelling',
    blob_loc='0_db/pl/meiro_environment/web_features/web_searches.csv',
    header=True,
    sep=',',
    infer_schema=False,
    encoding='utf-8'
)
web_searches = webSearches.process_data(sql_code="""
    SELECT 
        customer_entity_id,
        to_timestamp(substring_index(search_dt, '.', 1)) as search_dt,
        keywords,
        n_results
    FROM tmp_data
""")
# web_searches.show()
web_searches.createOrReplaceTempView('web_searches')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Features Processing

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Product Purchased in Last 1 - 12 Month

# COMMAND ----------

tmp_sql = """
SELECT 
    customer_entity_id,
    product_id,
    purchase_dt,
    IFNULL(sum(quantity * last1m_purchases), 0) as purchased_quantity_last1m,
    IFNULL(sum(quantity * last3m_purchases), 0) as purchased_quantity_last3m,
    IFNULL(sum(quantity * last6m_purchases), 0) as purchased_quantity_last6m,
    IFNULL(sum(quantity * last12m_purchases), 0) as purchased_quantity_last12m
FROM 
(
    SELECT
        trg.customer_entity_id,
        trg.product_id,
        trg.purchase_dt,
        case when months_between(trg.purchase_dt, m1.purchase_dt) between 0 and 1 then 1 else 0 end as last1m_purchases,
        case when months_between(trg.purchase_dt, m1.purchase_dt) between 0 and 3 then 1 else 0 end as last3m_purchases,
        case when months_between(trg.purchase_dt, m1.purchase_dt) between 0 and 6 then 1 else 0 end as last6m_purchases,
        case when months_between(trg.purchase_dt, m1.purchase_dt) between 0 and 12 then 1 else 0 end as last12m_purchases,
        IFNULL(m1.quantity, 0) as quantity
    FROM replenishment_target trg
    
    left join transactional_quantity m1
        on trg.customer_entity_id = m1.customer_entity_id
            and trg.product_id = m1.product_id
            and months_between(trg.purchase_dt, m1.purchase_dt) > 0 
            and months_between(trg.purchase_dt, m1.purchase_dt) <= 12
)
group by 1, 2, 3
"""
product_purchased_lastm = spark.sql(tmp_sql)
product_purchased_lastm.createOrReplaceTempView('product_purchased_lastm')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### TOP5 UTM Sources Last Month

# COMMAND ----------

tmp_sql = """
SELECT
    customer_entity_id,
    product_id,
    purchase_dt,
    utm_source,
    utm_source_id,
    count(distinct last1m_utm_source) as last1m_utm_source,
    count(distinct last3m_utm_source) as last3m_utm_source,
    count(distinct last6m_utm_source) as last6m_utm_source,
    count(distinct last12m_utm_source) as last12m_utm_source
FROM
(
    SELECT
        mst1.customer_entity_id,
        mst1.product_id,
        mst1.purchase_dt,
        mst1.utm_source,
        mst1.utm_source_id,
        case when months_between(mst1.purchase_dt, src.source_dt) between 0 and 1 then mst1.utm_source end as last1m_utm_source,
        case when months_between(mst1.purchase_dt, src.source_dt) between 0 and 3 then mst1.utm_source end as last3m_utm_source,
        case when months_between(mst1.purchase_dt, src.source_dt) between 0 and 6 then mst1.utm_source end as last6m_utm_source,
        case when months_between(mst1.purchase_dt, src.source_dt) between 0 and 12 then mst1.utm_source end as last12m_utm_source
    FROM
    (
        SELECT
            trg.customer_entity_id,
            trg.product_id,
            trg.purchase_dt,
            IFNULL(mst.utm_source, 'unknown') as utm_source,
            concat('source_', mst.utm_source_id) as utm_source_id
        FROM
        (
            SELECT 
                *,
                row_number() over (partition by whole_df order by n_rows desc) as utm_source_id
            FROM
            (
                SELECT 
                    utm_source, 
                    1 as whole_df,
                    count(*) as n_rows 
                FROM utm_source 
                GROUP BY utm_source
            ) 
            ORDER BY n_rows desc
            LIMIT 5
        ) mst
        CROSS JOIN replenishment_target trg
    ) mst1
    
    left join utm_source src
        on mst1.customer_entity_id = src.customer_entity_id
            and mst1.utm_source = src.utm_source
            and months_between(mst1.purchase_dt, src.source_dt) between 0 and 12
)
group by 1, 2, 3, 4, 5
"""

unpivot_exp = ("stack(4, 'last1m_utm_source', last1m_utm_source, 'last3m_utm_source', last3m_utm_source, " +
               "'last6m_utm_source', last6m_utm_source, 'last12m_utm_source', last12m_utm_source) as (" +
               "utm_var, utm_count)")

tmp_utm_source_lastm = (spark
                        .sql(tmp_sql)
                        .select('*', F.expr(unpivot_exp))
                        .drop('last1m_utm_source', 'last3m_utm_source',
                              'last6m_utm_source', 'last12m_utm_source'))
tmp_utm_source_lastm.createOrReplaceTempView('tmp_utm_source_lastm')

utm_source_lastm = (spark.sql("""
SELECT 
    customer_entity_id, 
    product_id, 
    purchase_dt, 
    concat(concat(utm_source_id, '__'), utm_var) as utm_variable,
    utm_count as utm_val
FROM tmp_utm_source_lastm
""").groupBy('customer_entity_id', 'product_id', 'purchase_dt')
                    .pivot('utm_variable')
                    .agg(F.sum('utm_val')))
# utm_source_lastm.show()
utm_source_lastm.createOrReplaceTempView('utm_source_lastm')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### TOP10 UTM Medium Last Month

# COMMAND ----------

tmp_sql = """
SELECT
    customer_entity_id,
    product_id,
    purchase_dt,
    utm_medium,
    utm_medium_id,
    count(DISTINCT last1m_utm_medium) AS last1m_utm_medium,
    count(DISTINCT last3m_utm_medium) AS last3m_utm_medium,
    count(DISTINCT last6m_utm_medium) AS last6m_utm_medium,
    count(DISTINCT last12m_utm_medium) AS last12m_utm_medium
FROM
(
    SELECT
        mst1.customer_entity_id,
        mst1.product_id,
        mst1.purchase_dt,
        mst1.utm_medium,
        mst1.utm_medium_id,
        CASE WHEN months_between(mst1.purchase_dt, src.medium_dt) BETWEEN 0 AND 1 THEN mst1.utm_medium END AS last1m_utm_medium,
        CASE WHEN months_between(mst1.purchase_dt, src.medium_dt) BETWEEN 0 AND 3 THEN mst1.utm_medium END AS last3m_utm_medium,
        CASE WHEN months_between(mst1.purchase_dt, src.medium_dt) BETWEEN 0 AND 6 THEN mst1.utm_medium END AS last6m_utm_medium,
        CASE WHEN months_between(mst1.purchase_dt, src.medium_dt) BETWEEN 0 AND 12 THEN mst1.utm_medium END AS last12m_utm_medium
    FROM
    (
        SELECT
            trg.customer_entity_id,
            trg.product_id,
            trg.purchase_dt,
            IFNULL(mst.utm_medium, 'unknown') as utm_medium,
            concat('medium_', mst.utm_medium_id) as utm_medium_id
        FROM
        (
            SELECT 
                *,
                row_number() OVER (PARTITION BY whole_df ORDER BY n_rows DESC) AS utm_medium_id
            FROM
            (
                SELECT 
                    utm_medium, 
                    1 as whole_df,
                    count(*) AS n_rows 
                FROM utm_medium
                GROUP BY utm_medium
            ) 
            ORDER BY n_rows DESC
            LIMIT 10
        ) mst
        CROSS JOIN replenishment_target trg
    ) mst1
    
    LEFT JOIN utm_medium src
        ON mst1.customer_entity_id = src.customer_entity_id
            AND mst1.utm_medium = src.utm_medium
            AND months_between(mst1.purchase_dt, src.medium_dt) BETWEEN 0 AND 12
)
GROUP BY 1, 2, 3, 4, 5
"""

unpivot_exp = ("stack(4, 'last1m_utm_medium', last1m_utm_medium, 'last3m_utm_medium', last3m_utm_medium, " +
               "'last6m_utm_medium', last6m_utm_medium, 'last12m_utm_medium', last12m_utm_medium) as (" +
               "utm_var, utm_count)")

tmp_utm_medium_lastm = (spark
                        .sql(tmp_sql)
                        .select('*', F.expr(unpivot_exp))
                        .drop('last1m_utm_medium', 'last3m_utm_medium',
                              'last6m_utm_medium', 'last12m_utm_medium'))
tmp_utm_medium_lastm.createOrReplaceTempView('tmp_utm_medium_lastm')

utm_medium_lastm = (spark.sql("""
SELECT 
    customer_entity_id, 
    product_id, 
    purchase_dt, 
    concat(concat(utm_medium_id, '__'), utm_var) as utm_variable,
    utm_count as utm_val
FROM tmp_utm_medium_lastm
""").groupBy('customer_entity_id', 'product_id', 'purchase_dt')
                    .pivot('utm_variable')
                    .agg(F.sum('utm_val')))
# utm_medium_lastm.show()
utm_medium_lastm.createOrReplaceTempView('utm_medium_lastm')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### TOP50 UTM Campaigns Last Month

# COMMAND ----------

tmp_sql = """
SELECT
    customer_entity_id,
    product_id,
    purchase_dt,
    utm_campaign,
    utm_campaign_id,
    count(DISTINCT last1m_utm_campaign) AS last1m_utm_campaign,
    count(DISTINCT last3m_utm_campaign) AS last3m_utm_campaign,
    count(DISTINCT last6m_utm_campaign) AS last6m_utm_campaign,
    count(DISTINCT last12m_utm_campaign) AS last12m_utm_campaign
FROM
(
    SELECT
        mst1.customer_entity_id,
        mst1.product_id,
        mst1.purchase_dt,
        mst1.utm_campaign,
        mst1.utm_campaign_id,
        CASE WHEN months_between(mst1.purchase_dt, src.campaign_dt) BETWEEN 0 AND 1 THEN mst1.utm_campaign END AS last1m_utm_campaign,
        CASE WHEN months_between(mst1.purchase_dt, src.campaign_dt) BETWEEN 0 AND 3 THEN mst1.utm_campaign END AS last3m_utm_campaign,
        CASE WHEN months_between(mst1.purchase_dt, src.campaign_dt) BETWEEN 0 AND 6 THEN mst1.utm_campaign END AS last6m_utm_campaign,
        CASE WHEN months_between(mst1.purchase_dt, src.campaign_dt) BETWEEN 0 AND 12 THEN mst1.utm_campaign END AS last12m_utm_campaign
    FROM
    (
        SELECT
            trg.customer_entity_id,
            trg.product_id,
            trg.purchase_dt,
            ifnull(mst.utm_campaign, 'unknown') as utm_campaign,
            concat('campaign_', cmp_num) as utm_campaign_id
        FROM
        (
            SELECT 
                *,
                row_number() OVER (PARTITION BY whole_df ORDER BY n_rows DESC) AS cmp_num
            FROM
            (
                SELECT 
                    utm_campaign,
                    1 as whole_df, 
                    count(*) as n_rows 
                FROM utm_campaign
                WHERE utm_campaign IS NOT NULL
                GROUP BY utm_campaign
            ) 
            ORDER BY n_rows DESC
            LIMIT 10
        ) mst
        CROSS JOIN replenishment_target trg
    ) mst1
    
    LEFT JOIN utm_campaign src
        ON mst1.customer_entity_id = src.customer_entity_id
            AND mst1.utm_campaign = src.utm_campaign
            AND months_between(mst1.purchase_dt, src.campaign_dt) BETWEEN 0 AND 12
)
GROUP BY 1, 2, 3, 4, 5
"""

unpivot_exp = ("stack(4, 'last1m_utm_campaign', last1m_utm_campaign, 'last3m_utm_campaign', last3m_utm_campaign, " +
               "'last6m_utm_campaign', last6m_utm_campaign, 'last12m_utm_campaign', last12m_utm_campaign) as (" +
               "utm_var, utm_count)")

tmp_utm_campaign_lastm = (spark
                          .sql(tmp_sql)
                          .select('*', F.expr(unpivot_exp))
                          .drop('last1m_utm_campaign', 'last3m_utm_campaign',
                                'last6m_utm_campaign', 'last12m_utm_campaign'))
tmp_utm_campaign_lastm.createOrReplaceTempView('tmp_utm_campaign_lastm')

utm_campaign_lastm = (spark.sql("""
SELECT 
    customer_entity_id, 
    product_id, 
    purchase_dt, 
    concat(concat(utm_campaign_id, '__'), utm_var) as utm_variable,
    utm_count as utm_val
FROM tmp_utm_campaign_lastm
""").groupBy('customer_entity_id', 'product_id', 'purchase_dt')
                      .pivot('utm_variable')
                      .agg(F.sum('utm_val')))
# utm_campaign_lastm.show()
utm_campaign_lastm.createOrReplaceTempView('utm_campaign_lastm')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Web Features

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Product Views

# COMMAND ----------

tmp_sql = """
SELECT
    customer_entity_id,
    product_id,
    purchase_dt,
    SUM(last1m_product_view) as last1m_product_view,
    SUM(last3m_product_view) as last3m_product_view,
    SUM(last6m_product_view) as last6m_product_view,
    SUM(last12m_product_view) as last12m_product_view
FROM
(
    SELECT
        trg.customer_entity_id,
        trg.product_id,
        trg.purchase_dt,
        CASE WHEN months_between(trg.purchase_dt, vw.viewed_dt) BETWEEN 0 AND 1 THEN 1 ELSE 0 END AS last1m_product_view,
        CASE WHEN months_between(trg.purchase_dt, vw.viewed_dt) BETWEEN 0 AND 3 THEN 1 ELSE 0 END AS last3m_product_view,
        CASE WHEN months_between(trg.purchase_dt, vw.viewed_dt) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS last6m_product_view,
        CASE WHEN months_between(trg.purchase_dt, vw.viewed_dt) BETWEEN 0 AND 12 THEN 1 ELSE 0 END AS last12m_product_view
    FROM replenishment_target trg
    
    LEFT JOIN product_views vw
        ON trg.customer_entity_id = vw.customer_entity_id
        AND trg.product_id = vw.product_id
        AND months_between(trg.purchase_dt, vw.viewed_dt) BETWEEN 0 AND 12
)
GROUP BY 1, 2, 3
"""

product_views = spark.sql(tmp_sql)
# product_views.show()
product_views.createOrReplaceTempView('product_views')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### TOP5 Banner Clicked

# COMMAND ----------

tmp_sql = """
SELECT
    customer_entity_id,
    product_id,
    purchase_dt,
    banner_rid,
    banner_id,
    banner_name,
    sum(last1m_clicked_banner) AS last1m_clicked_banner,
    sum(last3m_clicked_banner) AS last3m_clicked_banner,
    sum(last6m_clicked_banner) AS last6m_clicked_banner,
    sum(last12m_clicked_banner) AS last12m_clicked_banner
FROM
(
    SELECT
        mst1.customer_entity_id,
        mst1.product_id,
        mst1.purchase_dt,
        mst1.banner_rid,
        mst1.banner_id,
        mst1.banner_name,
        CASE WHEN months_between(mst1.purchase_dt, src.clicked_dt) BETWEEN 0 AND 1 THEN 1 ELSE 0 END AS last1m_clicked_banner,
        CASE WHEN months_between(mst1.purchase_dt, src.clicked_dt) BETWEEN 0 AND 3 THEN 1 ELSE 0 END AS last3m_clicked_banner,
        CASE WHEN months_between(mst1.purchase_dt, src.clicked_dt) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS last6m_clicked_banner,
        CASE WHEN months_between(mst1.purchase_dt, src.clicked_dt) BETWEEN 0 AND 12 THEN 1 ELSE 0 END AS last12m_clicked_banner
    FROM
    (
        SELECT
            trg.customer_entity_id,
            trg.product_id,
            trg.purchase_dt,
            mst.banner_id,
            ifnull(mst.banner_name, 'unknown') as banner_name,
            concat('banner_', banner_num) as banner_rid
        FROM
        (
            SELECT 
                *,
                row_number() OVER (PARTITION BY whole_df ORDER BY n_rows DESC) AS banner_num
            FROM
            (
                SELECT 
                    banner_name,
                    banner_id,
                    1 as whole_df, 
                    count(*) as n_rows 
                FROM banner_clicked
                WHERE banner_name IS NOT NULL and banner_id IS NOT NULL
                GROUP BY banner_name, banner_id
            ) 
            ORDER BY n_rows DESC
            LIMIT 10
        ) mst
        CROSS JOIN replenishment_target trg
    ) mst1
    
    LEFT JOIN banner_clicked src
        ON mst1.customer_entity_id = src.customer_entity_id
            AND mst1.banner_name = src.banner_name
            AND months_between(mst1.purchase_dt, src.clicked_dt) BETWEEN 0 AND 12
)
GROUP BY 1, 2, 3, 4, 5, 6
"""

unpivot_exp = (
        "stack(4, 'last1m_clicked_banner', last1m_clicked_banner, 'last3m_clicked_banner', last3m_clicked_banner, " +
        "'last6m_clicked_banner', last6m_clicked_banner, 'last12m_clicked_banner', last12m_clicked_banner) as (" +
        "var, count)")

tmp_banner_clicked_lastm = (spark
                            .sql(tmp_sql)
                            .select('*', F.expr(unpivot_exp))
                            .drop('last1m_clicked_banner', 'last3m_clicked_banner',
                                  'last6m_clicked_banner', 'last12m_clicked_banner'))
tmp_banner_clicked_lastm.createOrReplaceTempView('tmp_banner_clicked_lastm')

banner_clicked_lastm = (spark.sql("""
SELECT 
    customer_entity_id, 
    product_id, 
    purchase_dt, 
    concat(concat(banner_rid, '__'), var) as banner_variable,
    count as banner_val
FROM tmp_banner_clicked_lastm
""").groupBy('customer_entity_id', 'product_id', 'purchase_dt')
                        .pivot('banner_variable')
                        .agg(F.sum('banner_val')))
# banner_clicked_lastm.show()
banner_clicked_lastm.createOrReplaceTempView('banner_clicked_lastm')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### TOP5 Banner Impression

# COMMAND ----------

tmp_sql = """
SELECT
    customer_entity_id,
    product_id,
    purchase_dt,
    banner_rid,
    banner_id,
    banner_name,
    sum(last1m_impression_banner) AS last1m_impression_banner,
    sum(last3m_impression_banner) AS last3m_impression_banner,
    sum(last6m_impression_banner) AS last6m_impression_banner,
    sum(last12m_impression_banner) AS last12m_impression_banner
FROM
(
    SELECT
        mst1.customer_entity_id,
        mst1.product_id,
        mst1.purchase_dt,
        mst1.banner_rid,
        mst1.banner_id,
        mst1.banner_name,
        CASE WHEN months_between(mst1.purchase_dt, src.impression_dt) BETWEEN 0 AND 1 THEN 1 ELSE 0 END AS last1m_impression_banner,
        CASE WHEN months_between(mst1.purchase_dt, src.impression_dt) BETWEEN 0 AND 3 THEN 1 ELSE 0 END AS last3m_impression_banner,
        CASE WHEN months_between(mst1.purchase_dt, src.impression_dt) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS last6m_impression_banner,
        CASE WHEN months_between(mst1.purchase_dt, src.impression_dt) BETWEEN 0 AND 12 THEN 1 ELSE 0 END AS last12m_impression_banner
    FROM
    (
        SELECT
            trg.customer_entity_id,
            trg.product_id,
            trg.purchase_dt,
            mst.banner_id,
            ifnull(mst.banner_name, 'unknown') as banner_name,
            concat('banner_', banner_num) as banner_rid
        FROM
        (
            SELECT 
                *,
                row_number() OVER (PARTITION BY whole_df ORDER BY n_rows DESC) AS banner_num
            FROM
            (
                SELECT 
                    banner_name,
                    banner_id,
                    1 as whole_df, 
                    count(*) as n_rows 
                FROM banner_impression
                WHERE banner_name IS NOT NULL and banner_id IS NOT NULL
                GROUP BY banner_name, banner_id
            ) 
            ORDER BY n_rows DESC
            LIMIT 10
        ) mst
        CROSS JOIN replenishment_target trg
    ) mst1
    
    LEFT JOIN banner_impression src
        ON mst1.customer_entity_id = src.customer_entity_id
            AND mst1.banner_name = src.banner_name
            AND months_between(mst1.purchase_dt, src.impression_dt) BETWEEN 0 AND 12
)
GROUP BY 1, 2, 3, 4, 5, 6
"""

unpivot_exp = (
        "stack(4, 'last1m_impression_banner', last1m_impression_banner, 'last3m_impression_banner', last3m_impression_banner, " +
        "'last6m_impression_banner', last6m_impression_banner, 'last12m_impression_banner', last12m_impression_banner) as (" +
        "var, count)")

tmp_banner_impression_lastm = (spark
                               .sql(tmp_sql)
                               .select('*', F.expr(unpivot_exp))
                               .drop('last1m_impression_banner', 'last3m_impression_banner',
                                     'last6m_impression_banner', 'last12m_impression_banner'))
tmp_banner_impression_lastm.createOrReplaceTempView('tmp_banner_impression_lastm')

banner_impression_lastm = (spark.sql("""
SELECT 
    customer_entity_id, 
    product_id, 
    purchase_dt, 
    concat(concat(banner_rid, '__'), var) as banner_variable,
    count as banner_val
FROM tmp_banner_impression_lastm
""").groupBy('customer_entity_id', 'product_id', 'purchase_dt')
                           .pivot('banner_variable')
                           .agg(F.sum('banner_val')))
# banner_impression_lastm.show()
banner_impression_lastm.createOrReplaceTempView('banner_impression_lastm')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Modelling Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Merging Data

# COMMAND ----------

tmp_sql = """
SELECT 
    -- TARGET
    trg.customer_entity_id,
    trg.product_id,
    trg.purchase_dt,
    trg.last_purchase_dt,
    trg.days_since_last_purchase,
    log(trg.days_since_last_purchase) as log_days_since_last_purchase,
    avg(trg.days_since_last_purchase) over(partition by trg.product_id) as longterm_average_since_last_purchase,
    idi.n_identifiers,
    
    -- PRODUCT
    -- prd.item_id,
    IFNULL(prd.first_category, 'Unknown') AS first_category,
    IFNULL(prd.second_category, 'Unknown') AS second_category,
    IFNULL(prd.third_category, 'Unknown') AS third_category,
    IFNULL(prd.brand, 'Unknown') AS brand,
    IFNULL(prd.price, 0) AS price,
    
    -- IDENTITY
    -- ida.customer_entity_id,
    IFNULL(ida.hours_since__first_event, 0) AS hours_since__first_event,
    IFNULL(ida.hours_since__modified, 0) AS hours_since__modified,
    IFNULL(ida.hours_since__calculated, 0) AS hours_since__calculated,
    IFNULL(ida.customer_events_count, 0) AS customer_events_count,
    IFNULL(ida.se_click_rate, 0) AS se_click_rate,
    IFNULL(ida.se_email_clicked_count, 0) AS se_email_clicked_count,
    IFNULL(ida.hours_since__se_email_clicked_date_last, 0) AS hours_since__se_email_clicked_date_last,
    IFNULL(ida.se_email_read_count, 0) AS se_email_read_count,
    IFNULL(ida.hours_since__se_email_read_date_last, 0) AS hours_since__se_email_read_date_last,
    IFNULL(ida.se_email_sent_count, 0) AS se_email_sent_count,
    IFNULL(ida.hours_since__se_email_sent_date_last, 0) AS hours_since__se_email_sent_date_last,
    IFNULL(ida.se_engagement, 0) AS se_engagement,
    IFNULL(ida.se_events_count, 0) AS se_events_count,
    IFNULL(ida.se_open_rate, 0) AS se_open_rate,
    IFNULL(ida.me_events_count, 0) AS me_events_count,
    IFNULL(ida.me_visits_count, 0) AS me_visits_count,
    IFNULL(ida.me_os_most_freq, 'Unknown') AS me_os_most_freq,
    IFNULL(ida.me_page_views_avg, 0) AS me_page_views_avg,
    IFNULL(ida.me_amount_spent_all, 0) AS me_amount_spent_all,
    IFNULL(ida.me_browser_most_freq, 'Unknown') AS me_browser_most_freq,
    IFNULL(ida.me_device_type_most_freq, 'Unknown') AS me_device_type_most_freq,
    IFNULL(ida.me_device_model_most_freq, 'Unknown') AS me_device_model_most_freq,
    IFNULL(ida.me_device_brand_most_freq, 'Unknown') AS me_device_brand_most_freq,
    IFNULL(ida.me_country, 'Unknown') AS me_country,
    IFNULL(ida.me_city, 'Unknown') AS me_city,
    IFNULL(ida.me_city_last, 'Unknown') AS me_city_last,
    IFNULL(ida.me_payment_most_freq, 'Unknown') AS me_payment_most_freq,
    IFNULL(ida.me_delivery_most_freq, 'Unknown') AS me_delivery_most_freq,
    IFNULL(ida.me_device_model_most_freq_second, 'Unknown') AS me_device_model_most_freq_second,
    IFNULL(ida.me_device_brand_most_freq_second, 'Unknown') AS me_device_brand_most_freq_second,
    IFNULL(ida.me_device_type_most_freq_second, 'Unknown') AS me_device_type_most_freq_second,
    IFNULL(ida.me_amount_spent_avg, 0) AS me_amount_spent_avg,
    IFNULL(ida.hours_since__me_transaction_datetime_first, 0) AS hours_since__me_transaction_datetime_first,
    IFNULL(ida.hours_since__me_transaction_datetime_last, 0) AS hours_since__me_transaction_datetime_last,
    IFNULL(ida.me_brand_purchase_most_freq, 'Unknown') AS me_brand_purchase_most_freq,
    IFNULL(ida.me_product_purchase_most_freq, 'Unknown') AS me_product_purchase_most_freq,
    IFNULL(ida.me_product_viewed_most_freq, 'Unknown') AS me_product_viewed_most_freq,
    IFNULL(ida.me_transaction_count, 0) AS me_transaction_count,
    IFNULL(ida.me_engagement, 0) AS me_engagement,
    IFNULL(ida.me_spend_percentile_quintile, 0) AS me_spend_percentile_quintile,
    IFNULL(ida.mx_gender_est_last, 'Unknown') AS mx_gender_est_last,
    IFNULL(ida.mx_email_count_unique, 0) AS mx_email_count_unique,
    IFNULL(ida.web_time_spent_sum, 0) AS web_time_spent_sum,
    IFNULL(ida.hours_since__me_registration_date, 0) AS hours_since__me_registration_date,
    
    -- ids.customer_entity_id,
    IFNULL(ids.magento, 0) AS magento,
    IFNULL(ids.me_web, 0) AS me_web,
    IFNULL(ids.se, 0) AS se,
    
    -- PRODUCT
    -- ppur.customer_entity_id,
    -- ppur.product_id,
    -- ppur.purchase_dt,
    IFNULL(ppur.purchased_quantity_last1m, 0) AS purchased_quantity_last1m,
    IFNULL(ppur.purchased_quantity_last3m, 0) AS purchased_quantity_last3m,
    IFNULL(ppur.purchased_quantity_last6m, 0) AS purchased_quantity_last6m,
    IFNULL(ppur.purchased_quantity_last12m, 0) AS purchased_quantity_last12m,
    
    -- BANNER
    -- clk.customer_entity_id,
    -- clk.product_id,
    -- clk.purchase_dt,
    IFNULL(clk.banner_10__last12m_clicked_banner, 0) AS banner_10__last12m_clicked_banner,
    IFNULL(clk.banner_10__last1m_clicked_banner, 0) AS banner_10__last1m_clicked_banner,
    IFNULL(clk.banner_10__last3m_clicked_banner, 0) AS banner_10__last3m_clicked_banner,
    IFNULL(clk.banner_10__last6m_clicked_banner, 0) AS banner_10__last6m_clicked_banner,
    IFNULL(clk.banner_1__last12m_clicked_banner, 0) AS banner_1__last12m_clicked_banner,
    IFNULL(clk.banner_1__last1m_clicked_banner, 0) AS banner_1__last1m_clicked_banner,
    IFNULL(clk.banner_1__last3m_clicked_banner, 0) AS banner_1__last3m_clicked_banner,
    IFNULL(clk.banner_1__last6m_clicked_banner, 0) AS banner_1__last6m_clicked_banner,
    IFNULL(clk.banner_2__last12m_clicked_banner, 0) AS banner_2__last12m_clicked_banner,
    IFNULL(clk.banner_2__last1m_clicked_banner, 0) AS banner_2__last1m_clicked_banner,
    IFNULL(clk.banner_2__last3m_clicked_banner, 0) AS banner_2__last3m_clicked_banner,
    IFNULL(clk.banner_2__last6m_clicked_banner, 0) AS banner_2__last6m_clicked_banner,
    IFNULL(clk.banner_3__last12m_clicked_banner, 0) AS banner_3__last12m_clicked_banner,
    IFNULL(clk.banner_3__last1m_clicked_banner, 0) AS banner_3__last1m_clicked_banner,
    IFNULL(clk.banner_3__last3m_clicked_banner, 0) AS banner_3__last3m_clicked_banner,
    IFNULL(clk.banner_3__last6m_clicked_banner, 0) AS banner_3__last6m_clicked_banner,
    IFNULL(clk.banner_4__last12m_clicked_banner, 0) AS banner_4__last12m_clicked_banner,
    IFNULL(clk.banner_4__last1m_clicked_banner, 0) AS banner_4__last1m_clicked_banner,
    IFNULL(clk.banner_4__last3m_clicked_banner, 0) AS banner_4__last3m_clicked_banner,
    IFNULL(clk.banner_4__last6m_clicked_banner, 0) AS banner_4__last6m_clicked_banner,
    IFNULL(clk.banner_5__last12m_clicked_banner, 0) AS banner_5__last12m_clicked_banner,
    IFNULL(clk.banner_5__last1m_clicked_banner, 0) AS banner_5__last1m_clicked_banner,
    IFNULL(clk.banner_5__last3m_clicked_banner, 0) AS banner_5__last3m_clicked_banner,
    IFNULL(clk.banner_5__last6m_clicked_banner, 0) AS banner_5__last6m_clicked_banner,
    IFNULL(clk.banner_6__last12m_clicked_banner, 0) AS banner_6__last12m_clicked_banner,
    IFNULL(clk.banner_6__last1m_clicked_banner, 0) AS banner_6__last1m_clicked_banner,
    IFNULL(clk.banner_6__last3m_clicked_banner, 0) AS banner_6__last3m_clicked_banner,
    IFNULL(clk.banner_6__last6m_clicked_banner, 0) AS banner_6__last6m_clicked_banner,
    IFNULL(clk.banner_7__last12m_clicked_banner, 0) AS banner_7__last12m_clicked_banner,
    IFNULL(clk.banner_7__last1m_clicked_banner, 0) AS banner_7__last1m_clicked_banner,
    IFNULL(clk.banner_7__last3m_clicked_banner, 0) AS banner_7__last3m_clicked_banner,
    IFNULL(clk.banner_7__last6m_clicked_banner, 0) AS banner_7__last6m_clicked_banner,
    IFNULL(clk.banner_8__last12m_clicked_banner, 0) AS banner_8__last12m_clicked_banner,
    IFNULL(clk.banner_8__last1m_clicked_banner, 0) AS banner_8__last1m_clicked_banner,
    IFNULL(clk.banner_8__last3m_clicked_banner, 0) AS banner_8__last3m_clicked_banner,
    IFNULL(clk.banner_8__last6m_clicked_banner, 0) AS banner_8__last6m_clicked_banner,
    IFNULL(clk.banner_9__last12m_clicked_banner, 0) AS banner_9__last12m_clicked_banner,
    IFNULL(clk.banner_9__last1m_clicked_banner, 0) AS banner_9__last1m_clicked_banner,
    IFNULL(clk.banner_9__last3m_clicked_banner, 0) AS banner_9__last3m_clicked_banner,
    IFNULL(clk.banner_9__last6m_clicked_banner, 0) AS banner_9__last6m_clicked_banner,
    
    -- imp.customer_entity_id,
    -- imp.product_id,
    -- imp.purchase_dt,
    IFNULL(imp.banner_10__last12m_impression_banner, 0) AS banner_10__last12m_impression_banner,
    IFNULL(imp.banner_10__last1m_impression_banner, 0) AS banner_10__last1m_impression_banner,
    IFNULL(imp.banner_10__last3m_impression_banner, 0) AS banner_10__last3m_impression_banner,
    IFNULL(imp.banner_10__last6m_impression_banner, 0) AS banner_10__last6m_impression_banner,
    IFNULL(imp.banner_1__last12m_impression_banner, 0) AS banner_1__last12m_impression_banner,
    IFNULL(imp.banner_1__last1m_impression_banner, 0) AS banner_1__last1m_impression_banner,
    IFNULL(imp.banner_1__last3m_impression_banner, 0) AS banner_1__last3m_impression_banner,
    IFNULL(imp.banner_1__last6m_impression_banner, 0) AS banner_1__last6m_impression_banner,
    IFNULL(imp.banner_2__last12m_impression_banner, 0) AS banner_2__last12m_impression_banner,
    IFNULL(imp.banner_2__last1m_impression_banner, 0) AS banner_2__last1m_impression_banner,
    IFNULL(imp.banner_2__last3m_impression_banner, 0) AS banner_2__last3m_impression_banner,
    IFNULL(imp.banner_2__last6m_impression_banner, 0) AS banner_2__last6m_impression_banner,
    IFNULL(imp.banner_3__last12m_impression_banner, 0) AS banner_3__last12m_impression_banner,
    IFNULL(imp.banner_3__last1m_impression_banner, 0) AS banner_3__last1m_impression_banner,
    IFNULL(imp.banner_3__last3m_impression_banner, 0) AS banner_3__last3m_impression_banner,
    IFNULL(imp.banner_3__last6m_impression_banner, 0) AS banner_3__last6m_impression_banner,
    IFNULL(imp.banner_4__last12m_impression_banner, 0) AS banner_4__last12m_impression_banner,
    IFNULL(imp.banner_4__last1m_impression_banner, 0) AS banner_4__last1m_impression_banner,
    IFNULL(imp.banner_4__last3m_impression_banner, 0) AS banner_4__last3m_impression_banner,
    IFNULL(imp.banner_4__last6m_impression_banner, 0) AS banner_4__last6m_impression_banner,
    IFNULL(imp.banner_5__last12m_impression_banner, 0) AS banner_5__last12m_impression_banner,
    IFNULL(imp.banner_5__last1m_impression_banner, 0) AS banner_5__last1m_impression_banner,
    IFNULL(imp.banner_5__last3m_impression_banner, 0) AS banner_5__last3m_impression_banner,
    IFNULL(imp.banner_5__last6m_impression_banner, 0) AS banner_5__last6m_impression_banner,
    IFNULL(imp.banner_6__last12m_impression_banner, 0) AS banner_6__last12m_impression_banner,
    IFNULL(imp.banner_6__last1m_impression_banner, 0) AS banner_6__last1m_impression_banner,
    IFNULL(imp.banner_6__last3m_impression_banner, 0) AS banner_6__last3m_impression_banner,
    IFNULL(imp.banner_6__last6m_impression_banner, 0) AS banner_6__last6m_impression_banner,
    IFNULL(imp.banner_7__last12m_impression_banner, 0) AS banner_7__last12m_impression_banner,
    IFNULL(imp.banner_7__last1m_impression_banner, 0) AS banner_7__last1m_impression_banner,
    IFNULL(imp.banner_7__last3m_impression_banner, 0) AS banner_7__last3m_impression_banner,
    IFNULL(imp.banner_7__last6m_impression_banner, 0) AS banner_7__last6m_impression_banner,
    IFNULL(imp.banner_8__last12m_impression_banner, 0) AS banner_8__last12m_impression_banner,
    IFNULL(imp.banner_8__last1m_impression_banner, 0) AS banner_8__last1m_impression_banner,
    IFNULL(imp.banner_8__last3m_impression_banner, 0) AS banner_8__last3m_impression_banner,
    IFNULL(imp.banner_8__last6m_impression_banner, 0) AS banner_8__last6m_impression_banner,
    IFNULL(imp.banner_9__last12m_impression_banner, 0) AS banner_9__last12m_impression_banner,
    IFNULL(imp.banner_9__last1m_impression_banner, 0) AS banner_9__last1m_impression_banner,
    IFNULL(imp.banner_9__last3m_impression_banner, 0) AS banner_9__last3m_impression_banner,
    IFNULL(imp.banner_9__last6m_impression_banner, 0) AS banner_9__last6m_impression_banner,
    
    -- Views
    -- vw.customer_entity_id,
    -- vw.product_id,
    -- vw.purchase_dt,
    vw.last1m_product_view,
    vw.last3m_product_view,
    vw.last6m_product_view,
    vw.last12m_product_view,
    
    -- UTM
    -- cmp.customer_entity_id,
    -- cmp.product_id,
    -- cmp.purchase_dt,
    IFNULL(cmp.campaign_10__last12m_utm_campaign, 0) AS campaign_10__last12m_utm_campaign,
    IFNULL(cmp.campaign_10__last1m_utm_campaign, 0) AS campaign_10__last1m_utm_campaign,
    IFNULL(cmp.campaign_10__last3m_utm_campaign, 0) AS campaign_10__last3m_utm_campaign,
    IFNULL(cmp.campaign_10__last6m_utm_campaign, 0) AS campaign_10__last6m_utm_campaign,
    IFNULL(cmp.campaign_1__last12m_utm_campaign, 0) AS campaign_1__last12m_utm_campaign,
    IFNULL(cmp.campaign_1__last1m_utm_campaign, 0) AS campaign_1__last1m_utm_campaign,
    IFNULL(cmp.campaign_1__last3m_utm_campaign, 0) AS campaign_1__last3m_utm_campaign,
    IFNULL(cmp.campaign_1__last6m_utm_campaign, 0) AS campaign_1__last6m_utm_campaign,
    IFNULL(cmp.campaign_2__last12m_utm_campaign, 0) AS campaign_2__last12m_utm_campaign,
    IFNULL(cmp.campaign_2__last1m_utm_campaign, 0) AS campaign_2__last1m_utm_campaign,
    IFNULL(cmp.campaign_2__last3m_utm_campaign, 0) AS campaign_2__last3m_utm_campaign,
    IFNULL(cmp.campaign_2__last6m_utm_campaign, 0) AS campaign_2__last6m_utm_campaign,
    IFNULL(cmp.campaign_3__last12m_utm_campaign, 0) AS campaign_3__last12m_utm_campaign,
    IFNULL(cmp.campaign_3__last1m_utm_campaign, 0) AS campaign_3__last1m_utm_campaign,
    IFNULL(cmp.campaign_3__last3m_utm_campaign, 0) AS campaign_3__last3m_utm_campaign,
    IFNULL(cmp.campaign_3__last6m_utm_campaign, 0) AS campaign_3__last6m_utm_campaign,
    IFNULL(cmp.campaign_4__last12m_utm_campaign, 0) AS campaign_4__last12m_utm_campaign,
    IFNULL(cmp.campaign_4__last1m_utm_campaign, 0) AS campaign_4__last1m_utm_campaign,
    IFNULL(cmp.campaign_4__last3m_utm_campaign, 0) AS campaign_4__last3m_utm_campaign,
    IFNULL(cmp.campaign_4__last6m_utm_campaign, 0) AS campaign_4__last6m_utm_campaign,
    IFNULL(cmp.campaign_5__last12m_utm_campaign, 0) AS campaign_5__last12m_utm_campaign,
    IFNULL(cmp.campaign_5__last1m_utm_campaign, 0) AS campaign_5__last1m_utm_campaign,
    IFNULL(cmp.campaign_5__last3m_utm_campaign, 0) AS campaign_5__last3m_utm_campaign,
    IFNULL(cmp.campaign_5__last6m_utm_campaign, 0) AS campaign_5__last6m_utm_campaign,
    IFNULL(cmp.campaign_6__last12m_utm_campaign, 0) AS campaign_6__last12m_utm_campaign,
    IFNULL(cmp.campaign_6__last1m_utm_campaign, 0) AS campaign_6__last1m_utm_campaign,
    IFNULL(cmp.campaign_6__last3m_utm_campaign, 0) AS campaign_6__last3m_utm_campaign,
    IFNULL(cmp.campaign_6__last6m_utm_campaign, 0) AS campaign_6__last6m_utm_campaign,
    IFNULL(cmp.campaign_7__last12m_utm_campaign, 0) AS campaign_7__last12m_utm_campaign,
    IFNULL(cmp.campaign_7__last1m_utm_campaign, 0) AS campaign_7__last1m_utm_campaign,
    IFNULL(cmp.campaign_7__last3m_utm_campaign, 0) AS campaign_7__last3m_utm_campaign,
    IFNULL(cmp.campaign_7__last6m_utm_campaign, 0) AS campaign_7__last6m_utm_campaign,
    IFNULL(cmp.campaign_8__last12m_utm_campaign, 0) AS campaign_8__last12m_utm_campaign,
    IFNULL(cmp.campaign_8__last1m_utm_campaign, 0) AS campaign_8__last1m_utm_campaign,
    IFNULL(cmp.campaign_8__last3m_utm_campaign, 0) AS campaign_8__last3m_utm_campaign,
    IFNULL(cmp.campaign_8__last6m_utm_campaign, 0) AS campaign_8__last6m_utm_campaign,
    IFNULL(cmp.campaign_9__last12m_utm_campaign, 0) AS campaign_9__last12m_utm_campaign,
    IFNULL(cmp.campaign_9__last1m_utm_campaign, 0) AS campaign_9__last1m_utm_campaign,
    IFNULL(cmp.campaign_9__last3m_utm_campaign, 0) AS campaign_9__last3m_utm_campaign,
    IFNULL(cmp.campaign_9__last6m_utm_campaign, 0) AS campaign_9__last6m_utm_campaign,
    
    -- med.customer_entity_id,
    -- med.product_id,
    -- med.purchase_dt,
    IFNULL(med.medium_10__last12m_utm_medium, 0) AS medium_10__last12m_utm_medium,
    IFNULL(med.medium_10__last1m_utm_medium, 0) AS medium_10__last1m_utm_medium,
    IFNULL(med.medium_10__last3m_utm_medium, 0) AS medium_10__last3m_utm_medium,
    IFNULL(med.medium_10__last6m_utm_medium, 0) AS medium_10__last6m_utm_medium,
    IFNULL(med.medium_1__last12m_utm_medium, 0) AS medium_1__last12m_utm_medium,
    IFNULL(med.medium_1__last1m_utm_medium, 0) AS medium_1__last1m_utm_medium,
    IFNULL(med.medium_1__last3m_utm_medium, 0) AS medium_1__last3m_utm_medium,
    IFNULL(med.medium_1__last6m_utm_medium, 0) AS medium_1__last6m_utm_medium,
    IFNULL(med.medium_2__last12m_utm_medium, 0) AS medium_2__last12m_utm_medium,
    IFNULL(med.medium_2__last1m_utm_medium, 0) AS medium_2__last1m_utm_medium,
    IFNULL(med.medium_2__last3m_utm_medium, 0) AS medium_2__last3m_utm_medium,
    IFNULL(med.medium_2__last6m_utm_medium, 0) AS medium_2__last6m_utm_medium,
    IFNULL(med.medium_3__last12m_utm_medium, 0) AS medium_3__last12m_utm_medium,
    IFNULL(med.medium_3__last1m_utm_medium, 0) AS medium_3__last1m_utm_medium,
    IFNULL(med.medium_3__last3m_utm_medium, 0) AS medium_3__last3m_utm_medium,
    IFNULL(med.medium_3__last6m_utm_medium, 0) AS medium_3__last6m_utm_medium,
    IFNULL(med.medium_4__last12m_utm_medium, 0) AS medium_4__last12m_utm_medium,
    IFNULL(med.medium_4__last1m_utm_medium, 0) AS medium_4__last1m_utm_medium,
    IFNULL(med.medium_4__last3m_utm_medium, 0) AS medium_4__last3m_utm_medium,
    IFNULL(med.medium_4__last6m_utm_medium, 0) AS medium_4__last6m_utm_medium,
    IFNULL(med.medium_5__last12m_utm_medium, 0) AS medium_5__last12m_utm_medium,
    IFNULL(med.medium_5__last1m_utm_medium, 0) AS medium_5__last1m_utm_medium,
    IFNULL(med.medium_5__last3m_utm_medium, 0) AS medium_5__last3m_utm_medium,
    IFNULL(med.medium_5__last6m_utm_medium, 0) AS medium_5__last6m_utm_medium,
    IFNULL(med.medium_6__last12m_utm_medium, 0) AS medium_6__last12m_utm_medium,
    IFNULL(med.medium_6__last1m_utm_medium, 0) AS medium_6__last1m_utm_medium,
    IFNULL(med.medium_6__last3m_utm_medium, 0) AS medium_6__last3m_utm_medium,
    IFNULL(med.medium_6__last6m_utm_medium, 0) AS medium_6__last6m_utm_medium,
    IFNULL(med.medium_7__last12m_utm_medium, 0) AS medium_7__last12m_utm_medium,
    IFNULL(med.medium_7__last1m_utm_medium, 0) AS medium_7__last1m_utm_medium,
    IFNULL(med.medium_7__last3m_utm_medium, 0) AS medium_7__last3m_utm_medium,
    IFNULL(med.medium_7__last6m_utm_medium, 0) AS medium_7__last6m_utm_medium,
    IFNULL(med.medium_8__last12m_utm_medium, 0) AS medium_8__last12m_utm_medium,
    IFNULL(med.medium_8__last1m_utm_medium, 0) AS medium_8__last1m_utm_medium,
    IFNULL(med.medium_8__last3m_utm_medium, 0) AS medium_8__last3m_utm_medium,
    IFNULL(med.medium_8__last6m_utm_medium, 0) AS medium_8__last6m_utm_medium,
    IFNULL(med.medium_9__last12m_utm_medium, 0) AS medium_9__last12m_utm_medium,
    IFNULL(med.medium_9__last1m_utm_medium, 0) AS medium_9__last1m_utm_medium,
    IFNULL(med.medium_9__last3m_utm_medium, 0) AS medium_9__last3m_utm_medium,
    IFNULL(med.medium_9__last6m_utm_medium, 0) AS medium_9__last6m_utm_medium,
    
    -- src.customer_entity_id,
    -- src.product_id,
    -- src.purchase_dt,
    IFNULL(src.source_1__last12m_utm_source, 0) AS source_1__last12m_utm_source,
    IFNULL(src.source_1__last1m_utm_source, 0) AS source_1__last1m_utm_source,
    IFNULL(src.source_1__last3m_utm_source, 0) AS source_1__last3m_utm_source,
    IFNULL(src.source_1__last6m_utm_source, 0) AS source_1__last6m_utm_source,
    IFNULL(src.source_2__last12m_utm_source, 0) AS source_2__last12m_utm_source,
    IFNULL(src.source_2__last1m_utm_source, 0) AS source_2__last1m_utm_source,
    IFNULL(src.source_2__last3m_utm_source, 0) AS source_2__last3m_utm_source,
    IFNULL(src.source_2__last6m_utm_source, 0) AS source_2__last6m_utm_source,
    IFNULL(src.source_3__last12m_utm_source, 0) AS source_3__last12m_utm_source,
    IFNULL(src.source_3__last1m_utm_source, 0) AS source_3__last1m_utm_source,
    IFNULL(src.source_3__last3m_utm_source, 0) AS source_3__last3m_utm_source,
    IFNULL(src.source_3__last6m_utm_source, 0) AS source_3__last6m_utm_source,
    IFNULL(src.source_4__last12m_utm_source, 0) AS source_4__last12m_utm_source,
    IFNULL(src.source_4__last1m_utm_source, 0) AS source_4__last1m_utm_source,
    IFNULL(src.source_4__last3m_utm_source, 0) AS source_4__last3m_utm_source,
    IFNULL(src.source_4__last6m_utm_source, 0) AS source_4__last6m_utm_source,
    IFNULL(src.source_5__last12m_utm_source, 0) AS source_5__last12m_utm_source,
    IFNULL(src.source_5__last1m_utm_source, 0) AS source_5__last1m_utm_source,
    IFNULL(src.source_5__last3m_utm_source, 0) AS source_5__last3m_utm_source,
    IFNULL(src.source_5__last6m_utm_source, 0) AS source_5__last6m_utm_source

from replenishment_target trg

left join identity_identifiers idi
    on trg.customer_entity_id = idi.customer_entity_id

left join products prd
    on trg.product_id = prd.item_id

left join identity_attributes ida
    on trg.customer_entity_id = ida.customer_entity_id

left join identity_sources ids
    on trg.customer_entity_id = ids.customer_entity_id

left join product_purchased_lastm ppur
    on trg.customer_entity_id = ppur.customer_entity_id
    and trg.product_id = ppur.product_id
    and trg.purchase_dt = ppur.purchase_dt

left join banner_clicked_lastm clk
    on trg.customer_entity_id = clk.customer_entity_id
    and trg.product_id = clk.product_id
    and trg.purchase_dt = clk.purchase_dt

left join banner_impression_lastm imp
    on trg.customer_entity_id = imp.customer_entity_id
    and trg.product_id = imp.product_id
    and trg.purchase_dt = imp.purchase_dt

left join product_views vw
    on trg.customer_entity_id = vw.customer_entity_id
    and trg.product_id = vw.product_id
    and trg.purchase_dt = vw.purchase_dt

left join utm_campaign_lastm cmp
    on trg.customer_entity_id = cmp.customer_entity_id
    and trg.product_id = cmp.product_id
    and trg.purchase_dt = cmp.purchase_dt

left join utm_medium_lastm med
    on trg.customer_entity_id = med.customer_entity_id
    and trg.product_id = med.product_id
    and trg.purchase_dt = med.purchase_dt

left join utm_source_lastm src
    on trg.customer_entity_id = src.customer_entity_id
    and trg.product_id = src.product_id
    and trg.purchase_dt = src.purchase_dt

where 
    trg.days_since_last_purchase > 20
    and ida.customer_entity_id is not null 
    and ids.customer_entity_id is not null
    and ppur.customer_entity_id is not null 
    and clk.customer_entity_id is not null 
    and imp.customer_entity_id is not null 
    and vw.customer_entity_id is not null 
    and cmp.customer_entity_id is not null 
    and med.customer_entity_id is not null 
    and src.customer_entity_id is not null
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write Parquet into BLOB STORAGE

# COMMAND ----------
BLOB_NAME = 'replenishment-modelling'
blob_file_nm = '1_model_df/group_replenishment_modelling/pl_modelling_data'
W_PATH = '{}/{}'.format(CONT_PATH.format(BLOB_NAME, STORAGE_ACCOUNT), blob_file_nm)

rem_blob_files(blob_loc=blob_file_nm,
               cont_nm=BLOB_NAME,
               block_blob_service=BLOCK_BLOB_SERVICE)

(spark.sql(tmp_sql)
 .write
 .mode('overwrite')
 .option('header', 'true')
 .format('parquet')
 .save(W_PATH))
