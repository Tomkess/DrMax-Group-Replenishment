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
from datetime import datetime
from typing import List, Any, Tuple

import pandas as pd
import pyspark.sql
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler, MinMaxScaler, MaxAbsScaler, Normalizer, VectorAssembler, OneHotEncoder, \
    StringIndexer, UnivariateFeatureSelector
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import log, col, when

if platform.uname().node == 'GL22WD4W2DY33':
    from src._initSparkCluster import *


class ElasticNetCV:

    def __init__(self,
                 blob_name: str,
                 blob_loc: str,
                 storage_account: str,
                 label: str,
                 cat_features: list,
                 num_features: list,
                 metric_name: str,
                 scaler_name: str,
                 log_transform: bool,

                 alpha: list,
                 l1_ratio: list,
                 top_features: int,
                 cv: int,
                 seed: int) -> None:

        # Features
        self.label = label
        self.cat_features = cat_features
        self.num_features = num_features
        self.master_features_db = None

        # ElaticNet parameters
        self.alpha = alpha
        self.l1_ratio = l1_ratio

        # Cross validation
        self.cv = cv
        self.cv_model = None
        self.master_data_after_pipeline = None

        # Location of data
        self.storage_account = storage_account
        self.blob_loc = blob_loc
        self.blob_name = blob_name

        # Data
        self.data = None

        # Apply log transformation
        self.log_transform = log_transform

        # Model
        self.model = None
        self.metric_name = metric_name

        # Scaler
        self.scaler_name = scaler_name
        self.sclr = None

        # Vectorizer
        self.vectorizer_model = None

        # Feature selector
        self.feature_selector_model = None
        self.top_features = top_features
        self.paramGrid = None

        # Encoding categorical features
        self.encoder = None
        self.indexers = None
        self.one_hot_encoded_features = None

        # Pipeline
        self.pipeline_model = None

        # Seed
        self.seed = seed

    def read_parquet(self) -> pyspark.sql.DataFrame:
        """This function reads parquet data from blob storage using pyspark.
        :return:
            DataFrame
        """

        # read parquet data
        tmp_data = spark.read.parquet('{}/{}'.format(CONT_PATH.format(self.blob_name, self.storage_account),
                                                     self.blob_loc))
        # self.data = tmp_data

        # return data
        print('Data read from blob storage!')
        return tmp_data

    def apply_log(self):
        """This function applies log transformation on numerical attributes.
        :return:
            DataFrame
        """
        # create temporary view
        tmp_data = self.read_parquet()

        # apply log transformation
        for num in self.num_features:
            new_nm = 'log__' + num
            tmp_data = tmp_data.withColumn(new_nm, when(col(num) <= 0, 0).otherwise(log(col(num))))

        print('Log transformation applied on feature space!')
        self.data = tmp_data
        return self.data

    def vectorizer(self, ifeatures: list) -> VectorAssembler:
        """This function vectorizes data.
        :return:
            pyspark.ml.feature.VectorAssembler
        """

        # define vectorizer
        vectorizer_model = VectorAssembler(inputCols=ifeatures, outputCol='features')
        print('Vectorizer - Assemble multiple columns into one!')
        self.vectorizer_model = vectorizer_model

        return vectorizer_model

    def normalizer(self) -> Normalizer:
        """This function normalizes data.
        :return:
            pyspark.ml.feature.Normalizer
        """

        # define normalizer
        normalizer = Normalizer(inputCol='features', outputCol='scaled_features')
        self.sclr = normalizer
        print('Normalizer - Selected for scaling features!')

        # return normalizer
        return normalizer

    def standard_scaler(self) -> StandardScaler:
        """This function scales data.
        :return:
            pyspark.ml.feature.StandardScalerModel
        """

        # define scaler
        sclr = StandardScaler(inputCol='features', outputCol='scaled_features')
        self.sclr = sclr
        print('StandardScaler - Selected for scaling features!')

        # return scaler
        return sclr

    def minmax_scaler(self) -> MinMaxScaler:
        """This function scales data.
        :return:
            pyspark.ml.feature.MinMaxScalerModel
        """

        # define scaler
        sclr = MinMaxScaler(inputCol='features', outputCol='scaled_features')
        self.sclr = sclr
        print('MinMaxScaler - Selected for scaling features!')

        # return scaler
        return sclr

    def maxabs_scaler(self) -> MaxAbsScaler:
        """This function scales data.
        :return:
            pyspark.ml.feature.StandardScalerModel
        """

        # define scaler
        sclr = MaxAbsScaler(inputCol='features', outputCol='scaled_features')
        self.sclr = sclr
        print('MaxAbsScaler - Selected for scaling features!')

        # return scaler
        return sclr

    def one_hot_encoder(self) -> Tuple[List[str], List[StringIndexer], OneHotEncoder]:
        """This function one hot encodes data.
        :return:
            pyspark.ml.feature.OneHotEncoderEstimator
        """

        # one-hot encoded features
        one_hot_encoded_features = ['{}__encoded'.format(col) for col in self.cat_features]
        self.one_hot_encoded_features = one_hot_encoded_features

        # create indexer for categorical variables so they can be one-hot encoded
        indexers = [StringIndexer(inputCol=col,
                                  outputCol='{}__index'.format(col),
                                  handleInvalid='keep') for col in self.cat_features]
        self.indexers = indexers

        # one-hot encode categorical features
        encoder = OneHotEncoder(inputCols=['{}__index'.format(col) for col in self.cat_features],
                                outputCols=one_hot_encoded_features)
        print('OneHotEncoder - Create indexers for categorical features and apply One Hot Encoder!')
        self.encoder = encoder

        # return encoder
        return one_hot_encoded_features, indexers, encoder

    def elastic_net_model(self) -> LinearRegression:
        """This function fits elastic net model.
        :return:
            pyspark.ml.regression.LinearRegression
        """

        elastic_net = LinearRegression(featuresCol='selected_features', labelCol=self.label, standardization=False)
        print('ElasticNet - Create Elastic Net Model!')
        self.model = elastic_net

        # return model
        return elastic_net

    def param_grid_builder(self) -> list[Any]:
        """This function builds parameter grid.
        :return:
            pyspark.ml.tuning.ParamGridBuilder
        """

        # define grid
        paramGrid = (ParamGridBuilder()
                     .addGrid(self.model.elasticNetParam, self.alpha)
                     .addGrid(self.model.regParam, self.l1_ratio)
                     .build())
        print('ParamGridBuilder - Create parameter grid!')
        self.paramGrid = paramGrid

        # return grid
        return paramGrid

    def feature_selector(self) -> UnivariateFeatureSelector:
        """This function selects features.
        :return:
            pyspark.ml.feature.UnivariateFeatureSelectorModel
        """

        # define feature selector
        selector = UnivariateFeatureSelector(selectionMode='numTopFeatures',
                                             labelCol=self.label,
                                             featuresCol='scaled_features',
                                             outputCol='selected_features')
        selector.setFeatureType('continuous').setLabelType('continuous').setSelectionThreshold(self.top_features)
        print('Feature selector - Selected for selecting features!')
        self.feature_selector_model = selector

        # return feature selector
        return selector

    def pipeliner(self) -> Tuple[pyspark.sql.DataFrame, PipelineModel]:
        """This function fits pipeline.
        :return:
            pyspark.ml.PipelineModel
        """

        # read data
        tmp_data = self.apply_log()

        # names of logarithmic features
        log_num_features = ['log__' + x for x in self.num_features]

        # one hot encode categorical features
        one_hot_encoded_features, indexers, encoder = self.one_hot_encoder()

        # vectorize all features
        if self.log_transform:
            vec_features = one_hot_encoded_features + log_num_features
        else:
            vec_features = one_hot_encoded_features + self.num_features
        vectorizer_model = self.vectorizer(ifeatures=vec_features)
        print(vec_features)

        # scaler
        sclr = self.minmax_scaler()
        if self.scaler_name == 'MinMaxScaler':
            sclr = self.minmax_scaler()
        if self.scaler_name == 'StandardScaler':
            sclr = self.standard_scaler()
        if self.scaler_name == 'MaxAbsScaler':
            sclr = self.maxabs_scaler()
        if self.scaler_name == 'Normalizer':
            sclr = self.normalizer()

        # feature selection model
        feature_selector = self.feature_selector()

        # define pipeline
        pipeline = Pipeline(stages=indexers + [encoder, vectorizer_model, sclr, feature_selector])
        pipeline_model = pipeline.fit(tmp_data)
        print('Pipeline - Create pipeline model!')
        self.pipeline_model = pipeline_model

        # return model
        return tmp_data, pipeline_model

    def __fit_cv_model(self) -> tuple[CrossValidatorModel, DataFrame]:
        """This function fits elastic net model with k fold cross validation.
        :return:
            pyspark.ml.tuning.CrossValidatorModel
        """

        # run pipeline
        tmp_data, pipeline_model = self.pipeliner()

        # create regression model
        model = self.elastic_net_model()

        # define grid
        self.param_grid_builder()

        # define evaluator
        evaluator = RegressionEvaluator(metricName=self.metric_name, labelCol=self.label)

        # define cross validator
        crossval = CrossValidator(estimator=model,
                                  estimatorParamMaps=self.paramGrid,
                                  evaluator=evaluator,
                                  numFolds=self.cv,
                                  seed=self.seed)

        # transform data
        master_data = pipeline_model.transform(tmp_data)

        # fit model
        cvModel = crossval.fit(master_data)
        print('CrossValidator - Fit model with cross validation!')

        # return model
        self.cv_model = cvModel
        self.master_data_after_pipeline = master_data
        return cvModel, master_data

    def get_feature_names(self):
        master_data = self.master_data_after_pipeline
        master_data.createOrReplaceTempView('master_data')

        # Categorical Features
        tmp_feature_db = []
        tmp_sql = """
        SELECT 
            {} as feature_name, 
            {} as feature_id, 
            count(*) as n_rows 
        from master_data 
        group by {}, {}
        """
        count = 0
        row_count = 0
        for cat in self.cat_features:

            if count == 0:
                f_data = spark.sql(tmp_sql.format(cat, cat + '__index', cat, cat + '__index')).toPandas()
                f_data['orig_feature_name'] = cat
                f_data['feature_type'] = 'categorical_attributes'
                tmp_feature_db.append(f_data)
            else:
                f_data = spark.sql(tmp_sql.format(cat, cat + '__index', cat, cat + '__index')).toPandas()
                f_data['orig_feature_name'] = cat
                f_data['feature_type'] = 'categorical_attributes'
                f_data['feature_id'] = f_data['feature_id'] + tmp_feature_db[count - 1]['feature_id'].max() + 1
                tmp_feature_db.append(f_data)

            row_count += f_data.shape[0]
            count += 1

        # Numerical Features
        if self.log_transform:
            num_features = ['log__' + x for x in self.num_features]
        else:
            num_features = self.num_features
        num_features_db = pd.DataFrame(data={'feature_name': num_features})
        num_features_db['orig_feature_name'] = num_features_db['feature_name']
        num_features_db['feature_type'] = 'numerical_attributes'
        num_features_db['feature_id'] = num_features_db.index + row_count
        num_features_db['n_rows'] = 0
        tmp_feature_db.append(num_features_db)

        # return feature names
        feature_db = pd.concat(tmp_feature_db, axis=0).reset_index(drop=True)
        return feature_db

    def fit(self):
        """This function fits elastic net model with k fold cross validation.
        :return:
            pyspark.ml.tuning.CrossValidatorModel
        """

        # fit model
        cv_model, master_data = self.__fit_cv_model()

        # get feature names
        feature_names = self.get_feature_names()
        feature_names['feature_id'] = feature_names['feature_id'].astype(int)

        if self.scaler_name == 'MinMaxScaler':
            feature_names['min'] = self.pipeline_model.stages[-2].originalMin.toArray()
            feature_names['max'] = self.pipeline_model.stages[-2].originalMax.toArray()

        feature_names = feature_names.drop(['n_rows'], axis=1).reset_index(drop=True)

        # get selected features
        selected_features = pd.DataFrame(data={'feature_id': self.pipeline_model.stages[-1].selectedFeatures})
        selected_features['coefficients'] = self.cv_model.bestModel.coefficients
        selected_features['intercept'] = self.cv_model.bestModel.intercept
        selected_features['scaler_name'] = self.scaler_name
        selected_features['estimation_dt'] = datetime.now()
        selected_features['is_selected'] = 1

        # merge feature names and selected features
        master_features = feature_names.merge(selected_features, on='feature_id', how='left')
        self.master_features_db = master_features

        # return model
        return cv_model, master_data, master_features

    def transform(self):
        """This function transforms data with fitted model.
        :return:
            pyspark.sql.dataframe.DataFrame
        """

        # from pyspark model get prediction on master_data
        master_data = self.master_data_after_pipeline
        cv_model = self.cv_model
        prediction = cv_model.transform(master_data)

        return prediction, master_data, cv_model
