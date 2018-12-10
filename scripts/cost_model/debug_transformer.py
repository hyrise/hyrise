#!/usr/bin/python

import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator
class Debug(BaseEstimator, TransformerMixin):

    def __init__(self, debug_location):
        self.debug_location = debug_location

    def transform(self, X):
        print(self.debug_location + ' - transform')
        print(list(pd.DataFrame(X)))
        #self.shape = X.shape
        #self.columns = list(pd.DataFrame(X))
        # what other output you want
        return X

    def fit(self, X, y=None, **fit_params):
        print(self.debug_location + ' - fit')
        print(list(pd.DataFrame(X)))
        return self
