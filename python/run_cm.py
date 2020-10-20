import joblib
import numpy as np
import os
import pandas as pd
from matplotlib import pyplot as plt

from cost_models import (
    add_dummy_types,
    equalize_data,
    import_data,
    train_cost_models,
    parse_cost_model_arguments,
    preprocess_data,
)
from prepare_calibration_data import parse_hyrise_csv
from visualization_utils import runtime_plots

TRAINING_DATA_PATH = os.path.join("..", "cmake-build-release", "data", "train")
TEST_DATA_PATH = os.path.join("..", "cmake-build-release", "data", "test")
MODEL_PATH = "cost_model_output"
train_data, test_data = import_data(TRAINING_DATA_PATH, TEST_DATA_PATH)
test_data = equalize_data(test_data)
train_data = equalize_data(train_data)
train_data["join"] = train_data["join"][train_data["join"]["OPERATOR_IMPLEMENTATION"] == "JoinHash"]
args = parse_cost_model_arguments(["-train", TRAINING_DATA_PATH, "--test", TEST_DATA_PATH])

train_cost_models(train_data, test_data, args)
