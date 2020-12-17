###########################################################################################################
# models return runtime predictions when <model>.predict(df) is called with one-hot encoded data frame
# steps to go:
#   1. load models and model input formats
#   2. select model based on operator, implementation, etc. (get_<operator>_model_name)
#   3. build df with features needed for model (see keep_labels in cost_models.py:get_<operator>_scores)
#   4. apply one-hot encoding to df (preprocess_data)
#   5. add possibly missing columns with append_to_input_format(df, input_formats(model_name))
#   6. call .predict(df) on selected model
#   7. enjoy
###########################################################################################################

import joblib
import numpy as np
import os
import pandas as pd

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge


def save_model_input_columns(df, file_name):
    with open(file_name, "w") as f:
        f.write(";".join(df.columns))


def get_join_model_name(model_type, implementation, build_column_type, probe_column_type):
    return f"{model_type}_Join_{implementation}_{build_column_type}_{probe_column_type}_model"


def get_table_scan_model_name(model_type, implementation):
    return f"{model_type}_TableScan_{implementation}_model"


def get_aggregate_model_name(model_type, implementation):
    return f"{model_type}_Aggregate_{implementation}_model"


def preprocess_data(data):
    # one-hot encoding
    ohe_candidates = [
        "OPERATOR_NAME",
        "COLUMN_TYPE",
        "DATA_TYPE",
        "ENCODING",
        "OPERATOR_IMPLEMENTATION",
        "COMPRESSION_TYPE",
        "SORTED",
        "JOIN_IMPLEMENTATION",
        "JOIN_MODE",
        "PREDICATE",
    ]
    ohe_columns = np.concatenate([[label for label in data.columns if keyword in label] for keyword in ohe_candidates])
    ohe_data = pd.get_dummies(data, columns=ohe_columns)
    return remove_dummy_types(ohe_data)


# needed for prediction with one-hot-encoding in case training and test data don't have the same set of values in a
# categorical data column
def add_dummy_types(train, test, cols):
    train["DUMMY"] = "0"
    test["DUMMY"] = "0"
    for col in cols:
        train_values = [
            value for value in train[train.DUMMY != "Dummy"][col].unique() if type(value) == str or not np.isnan(value)
        ]
        test_values = [
            value for value in test[test.DUMMY != "Dummy"][col].unique() if type(value) == str or not np.isnan(value)
        ]
        diff1 = np.setdiff1d(train_values, test_values)
        diff2 = np.setdiff1d(test_values, train_values)
        for d1 in diff1:
            test = test.append({"DUMMY": "Dummy", col: d1}, ignore_index=True)
        for d2 in diff2:
            train = train.append({"DUMMY": "Dummy", col: d2}, ignore_index=True)
    return [train, test]


def remove_dummy_types(data):
    if "DUMMY" in data:
        data = data[data["DUMMY"] != "Dummy"]
        data = data.drop(labels=["DUMMY"], axis=1)
    return data


def load_models(directory):
    file_extension = ".sav"
    models = dict()
    print(directory)
    model_file_names = [file_name for file_name in os.listdir(directory) if file_name.endswith(file_extension)]
    for file_name in model_file_names:
        models[file_name[: -len(file_extension)]] = joblib.load(os.path.join(directory, file_name))
    return models


def load_model_input_formats(directory):
    file_extension = "_columns.csv"
    formats = dict()
    model_file_names = [file_name for file_name in os.listdir(directory) if file_name.endswith(file_extension)]
    for file_name in model_file_names:
        formats[file_name[: -len(file_extension)]] = pd.read_csv(os.path.join(directory, file_name), sep=";")
    return formats


def append_to_input_format(df, input_format):
    result = df.copy()
    diffs = np.setdiff1d(input_format.columns, df.columns)
    for diff in diffs:
        print(f"adding column {diff}")
        result[diff] = 0
    control_diffs = np.setdiff1d(df.columns, input_format.columns)
    if len(control_diffs) > 0:
        print(f"dropping columns {', '.join(control_diffs)}")
        result = result.drop(columns=control_diffs)
        #raise ValueError(f"Did not expect column(s) {', '.join(control_diffs)}")
    return result[input_format.columns]
