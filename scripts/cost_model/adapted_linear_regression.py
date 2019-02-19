from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import LinearRegression
import pandas as pd

#HeteroscedasticLinearRegression
class HLinearRegression(BaseEstimator, RegressorMixin):

    def __init__(self):
        self.lr_ = LinearRegression(n_jobs=-1, fit_intercept=False, normalize=False)

    def fit(self, X, y):
        X = pd.DataFrame(X)
        y = pd.DataFrame(y)

        X = X.reset_index(drop=True)
        y = y.reset_index(drop=True)
        
        X_adj = X.div(y.execution_time_ns, axis='index')
        y_adj = y.div(y.execution_time_ns, axis='index')

        #X_adj = X.div(X.left_input_row_count, axis='index')
        #y_adj = y.div(X.left_input_row_count, axis='index')

        self.lr_.fit(X_adj, y_adj)
        return self

    def predict(self, X):
        return self.lr_.predict(X)

    def score(self, X, y, **kwargs):
        return self.lr_.score(X, y, )

    def lr(self):
        return self.lr_

    def coef(self):
        return self.lr_.coef_