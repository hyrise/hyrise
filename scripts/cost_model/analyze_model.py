#!/usr/bin/python

from sklearn.metrics import explained_variance_score, make_scorer, mean_absolute_error
from sklearn.metrics.scorer import neg_mean_absolute_error_scorer, explained_variance_scorer, r2_scorer, \
    neg_mean_squared_log_error_scorer, neg_mean_squared_error_scorer, neg_median_absolute_error_scorer
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

from sklearn.model_selection import cross_validate

import pandas as pd
import numpy as np
from math import sqrt

import warnings
warnings.filterwarnings('ignore')

def mean_absolute_percentage_error_impl(y, y_pred):
    return np.mean(np.abs((y - y_pred) / y))

def mean_percentage_error_impl(y, y_pred):
    return np.mean((y - y_pred) / y)

def root_mean_squared_error_impl(y, y_pred):
    return sqrt(mean_squared_error(y, y_pred))

def lnq_error_impl(y, y_pred):
    return np.mean(np.log(y_pred / y))

def normalized_root_mean_squared_error_impl(y, y_pred):
    return root_mean_squared_error_impl(y, y_pred) / np.mean(y)

class ModelAnalyzer:

    mean_absolute_percentage_error_scorer = make_scorer(
        score_func=mean_absolute_percentage_error_impl,
        greater_is_better=False)

    mean_percentage_error_scorer = make_scorer(
        score_func=mean_percentage_error_impl,
        greater_is_better=False)

    root_mean_squared_error_scorer = make_scorer(
        score_func=root_mean_squared_error_impl,
        greater_is_better=False)

    lnq_error_scorer = make_scorer(
        score_func=lnq_error_impl,
        greater_is_better=False)

    normalized_root_mean_squared_error_scorer = make_scorer(
        score_func=normalized_root_mean_squared_error_impl,
        greater_is_better=False)

    @staticmethod
    def mean_absolute_percentage_error(y, y_pred):
        return mean_absolute_percentage_error_impl(y, y_pred)

    @staticmethod
    def mean_percentage_error(y, y_pred):
        return mean_percentage_error_impl(y, y_pred)

    @staticmethod
    def root_mean_squared_error(y, y_pred):
        return root_mean_squared_error_impl(y, y_pred)

    @staticmethod
    def lnq_error(y, y_pred):
        return lnq_error_impl(y, y_pred)

    @staticmethod
    def normalized_root_mean_squared_error(y, y_pred):
        return normalized_root_mean_squared_error_impl(y, y_pred)

    @staticmethod
    def cross_validation(model_class, data):
        """

        Args:
            model_class:
            data: A dictionary containing keys X and Y

        Returns:
            a dict containing the scores

        """

        return cross_validate(
            model_class,
            X=data['X'],
            y=data['Y'],
            scoring={
                "explained_variance": explained_variance_scorer,
                "r2": r2_scorer,
                "mean_absolute_error": neg_mean_absolute_error_scorer,
                "mean_squared_error": neg_mean_squared_error_scorer,
                #"mean_squared_log_error": neg_mean_squared_log_error_scorer,
                "median_absolute_error": neg_median_absolute_error_scorer,
                "mean_absolute_percentage_error": ModelAnalyzer.mean_absolute_percentage_error_scorer,
                "mean_percentage_error": ModelAnalyzer.mean_percentage_error_scorer,
                "root_mean_squared_error": ModelAnalyzer.root_mean_squared_error_scorer,
                "normalized_root_mean_squared_error": ModelAnalyzer.normalized_root_mean_squared_error_scorer
            },
            cv=5,
            n_jobs=-1,
            return_train_score=False,
            return_estimator=True,
        )

    @staticmethod
    def evaluate(model, test_set_name, X_train, X_test, y_train, y_test):
        """

        Args:
            model:
            test_set_name:
            X_train:
            X_test:
            y_train:
            y_test:

        Returns:

        """
        model.fit(X_train, y_train)
        y_predicted = model.predict(X_test)

        explained_variance = explained_variance_score(y_test, y_predicted)
        mae = mean_absolute_error(y_test, y_predicted)
        mse = mean_squared_error(y_test, y_predicted)
        rmse = ModelAnalyzer.root_mean_squared_error(y_test, y_predicted)
        mape = ModelAnalyzer.mean_absolute_percentage_error(y_test, y_predicted)
        mpe = ModelAnalyzer.mean_percentage_error(y_test, y_predicted)
        nrmse = ModelAnalyzer.normalized_root_mean_squared_error(y_test, y_predicted)
        lnq = ModelAnalyzer.lnq_error(y_test, y_predicted)
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
            'lnq',
            'r2',
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
            lnq,
            r2
        )], columns=labels)

    @staticmethod
    def feature_importances(model, column_names):
        return pd.DataFrame([model.feature_importances_], columns=column_names)
