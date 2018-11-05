#!/usr/bin/python

from sklearn.metrics import explained_variance_score
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

import pandas as pd
import numpy as np
from math import sqrt

import warnings

warnings.filterwarnings('ignore')


class ModelAnalyzer:

    # def cross_validation(self, model_class, data, scores=[
    #     'explained_variance', 'r2', 'neg_mean_absolute_error', 'neg_mean_squared_error',
    #     'neg_mean_squared_log_error', 'neg_median_absolute_error']):
    # 
    #     a = np.array(data['Y'])
    #     print('Mean: ' + str(np.mean(a)))
    #     print('Median ' + str(np.median(a)))
    # 
    #     for score in scores:
    #         result = cross_val_score(model_class, data['X'], data['Y'], scoring=score, cv=10)
    #         print(model_class.__class__.__name__ + " - " + score + ":")
    #         print(str(np.array(result).mean()))

    @staticmethod
    def evaluate(model, test_set_name, X_train, X_test, y_train, y_test):
        model.fit(X_train, y_train)
        y_predicted = model.predict(X_test)

        explained_variance = explained_variance_score(y_test, y_predicted)
        mae = mean_absolute_error(y_test, y_predicted)
        mse = mean_squared_error(y_test, y_predicted)
        rmse = sqrt(mean_squared_error(y_test, y_predicted))
        mape = ModelAnalyzer.mean_absolute_percentage_error(y_test, y_predicted)
        mpe = ModelAnalyzer.mean_percentage_error(y_test, y_predicted)
        nrmse = ModelAnalyzer.normalized_root_mean_squared_error(y_test, y_predicted)
        r2 = r2_score(y_test, y_predicted)

        labels = [
            'model',
            'test_set_name',
            'explained variance score',
            'mean absolute error',
            'mean squared error',
            'root mean squared error',
            'mean absolute percentage error',
            'mean percentage error',
            'normalized rmse',
            'r2'
        ]

        return pd.DataFrame.from_records([(
            model.__class__.__name__,
            test_set_name,
            explained_variance,
            mae,
            mse,
            rmse,
            mape,
            mpe,
            nrmse,
            r2
        )], columns=labels)

    @staticmethod
    def feature_importances(model, column_names):
        return pd.DataFrame([model.feature_importances_], columns=column_names)

    @staticmethod
    def mean_absolute_percentage_error(y_true, y_pred):
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        return np.mean(np.abs((y_true - y_pred) / y_true))

    @staticmethod
    def mean_percentage_error(y_true, y_pred):
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        return np.mean((y_true - y_pred) / y_true)

    @staticmethod
    def normalized_root_mean_squared_error(y_true, y_pred):
        y_true, y_pred = np.array(y_true), np.array(y_pred)

        mse = mean_squared_error(y_true, y_pred)
        rmse = sqrt(mse)
        return rmse / np.mean(y_true)

    # def print_predictions(self, df, model, columns, label):
    #     import matplotlib.pyplot as plt
    #
    #     def predict(selectivity, columns):
    #         row = df.iloc[0].copy()
    #
    #         for name, column in row.iteritems():
    #             if name in columns:
    #                 row[name] = 1
    #             else:
    #                 row[name] = 0
    #
    #         row['output_selectivity'] = selectivity
    #         row_features = row.drop('execution_time_ns').values
    #         row_features = row_features.reshape(1, -1)
    #         predicted = model.predict(row_features)
    #         return predicted[0]
    #
    #     predictions = {s / 100.0: predict(s / 100.0, columns) for s in range(100)}
    #     plt.plot(predictions.keys(), predictions.values(), label=label)

    # def print_raw_data(self, df, columns, label):
    #     import matplotlib.pyplot as plt
    #     for column in columns:
    #         df = df[df[column] == 1]
    #     grouped = df.groupby(['output_selectivity'], as_index=False)['execution_time_ns'].mean()
    #     plt.plot(grouped['output_selectivity'], grouped['execution_time_ns'], label=label, linestyle="--")
