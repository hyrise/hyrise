import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline, make_union
from sklearn.preprocessing import PolynomialFeatures
from sklearn.svm import LinearSVR
from tpot.builtins import StackingEstimator
from tpot.export_utils import set_param_recursive

# NOTE: Make sure that the outcome column is labeled 'target' in the data file
tpot_data = pd.read_csv('PATH/TO/DATA/FILE', sep='COLUMN_SEPARATOR', dtype=np.float64)
features = tpot_data.drop('target', axis=1)
training_features, testing_features, training_target, testing_target = \
            train_test_split(features, tpot_data['target'], random_state=1337)

# Average CV score on the training set was: -927639348.0582241
exported_pipeline = make_pipeline(
    StackingEstimator(estimator=LinearSVR(C=0.001, dual=False, epsilon=0.0001, loss="squared_epsilon_insensitive", tol=1e-05)),
    PolynomialFeatures(degree=2, include_bias=False, interaction_only=False),
    RandomForestRegressor(bootstrap=False, max_features=0.25, min_samples_leaf=1, min_samples_split=2, n_estimators=100)
)
# Fix random state for all the steps in exported pipeline
set_param_recursive(exported_pipeline.steps, 'random_state', 1337)

exported_pipeline.fit(training_features, training_target)
results = exported_pipeline.predict(testing_features)
