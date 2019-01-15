import pandas as pd
from sklearn import preprocessing as pre
from training_data_pipeline import TrainingDataPipeline

import warnings

import numpy as np

from sklearn.linear_model import LinearRegression
from sklearn.base import BaseEstimator, RegressorMixin

warnings.filterwarnings('ignore')



class SpecializedModel:

    models = {}

    group_by_columns = [
            'first_column_segment_encoding', 
            'first_column_segment_data_type', 
            'is_column_comparison', 
            'first_column_is_segment_reference_segment',
        ]

    def __init__(self, model_type):
        self.model_type = model_type

    def fit(self, X, y):
        X = pd.DataFrame(X)
        y = pd.DataFrame(y)

        y_name = list(y)

        joined_df = X.join(y)
        grouped_dfs = {group: x for group, x in joined_df.groupby(self.group_by_columns)}

        def learn_model_for_group(df):
            X_new = df.drop(y_name, axis=1)
            y_new = df[y_name]

            model = self.model_type()
            model.fit(X_new, y_new)
            return model

        self.models = { group: learn_model_for_group(df) for group, df in grouped_dfs.items() }
        return self

    def predict(self, X):
        def predict_single_entry(row):
            group, x = row.groupby(self.group_by_columns)
            model = self.models[group]

            # todo: check that model exists

            return model.predict(x)

        return [predict_single_entry(row) for row in X]

