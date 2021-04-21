import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

# NOTE: Make sure that the outcome column is labeled 'target' in the data file
tpot_data = pd.read_csv('PATH/TO/DATA/FILE', sep='COLUMN_SEPARATOR', dtype=np.float64)
features = tpot_data.drop('target', axis=1)
training_features, testing_features, training_target, testing_target = \
            train_test_split(features, tpot_data['target'], random_state=1337)

# Average CV score on the training set was: -444921567.0316617
exported_pipeline = RandomForestRegressor(bootstrap=False, max_features=0.9500000000000001, min_samples_leaf=5, min_samples_split=20, n_estimators=100)
# Fix random state in exported estimator
if hasattr(exported_pipeline, 'random_state'):
    setattr(exported_pipeline, 'random_state', 1337)

exported_pipeline.fit(training_features, training_target)
results = exported_pipeline.predict(testing_features)
