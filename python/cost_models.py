#!/usr/bin/env python3

import argparse
import joblib
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import warnings

from prepare_calibration_data import import_operator_data
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from tpot import TPOTRegressor

plt.style.use("ggplot")


def preprocess_data(data):
    # one-hot encoding
    orig_cols = data.columns
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
    ]
    ohe_columns = np.concatenate([[label for label in data.columns if keyword in label] for keyword in ohe_candidates])
    ohe_data = pd.get_dummies(data, columns=ohe_columns)
    ohe_columns = ohe_data.columns
    b = remove_dummy_types(ohe_data)
    return b


def train_model(train_data, model_type):
    ohe_data = train_data.copy()
    y = np.ravel(ohe_data[["RUNTIME_NS"]])
    X = ohe_data.drop(labels=["RUNTIME_NS"], axis=1)
    tpot = TPOTRegressor(verbosity=1, random_state=1337, max_time_mins=300)
    tpot.fit(X, y)

    # if model_type == "linear":
    #    model = LinearRegression().fit(X, y)
    # elif model_type == "ridge":
    #    model = Ridge(alpha=10).fit(X, y)
    # elif model_type == "lasso":
    #    model = Lasso(alpha=10).fit(X, y)
    # elif model_type == "boost":
    #    model = GradientBoostingRegressor(loss="huber").fit(X, y)

    return tpot


def generate_model_plot(
    model,
    test_data,
    operator,
    method,
    encoding_left,
    encoding_right,
    implementation,
    left_column_type,
    right_column_type,
    data_type,
    out,
):
    ohe_data = test_data.copy()
    real_y = np.ravel(ohe_data[["RUNTIME_NS"]])
    ohe_data = ohe_data.drop(labels=["RUNTIME_NS"], axis=1)
    pred_y = model.predict(ohe_data)

    model_scores = calculate_error(ohe_data, pred_y, real_y, model)
    plt.scatter(real_y, pred_y, c="orange")
    axis_max = max(np.amax(pred_y), np.amax(real_y)) * 1.05
    axis_min = min(np.amin(pred_y), np.amin(real_y)) * 0.95
    abline_values = range(int(axis_min), int(axis_max), int((axis_max - axis_min) / 100))
    sample_size = "{:,}".format(ohe_data.shape[0]).replace(",", "'")
    plt.title(
        f'{encoding_left}_{encoding_right}_{operator}_{implementation}_{method}; Score: {model_scores["R2"]} ({sample_size} samples)'
    )
    plt.ylim([axis_min, axis_max])
    plt.xlim([axis_min, axis_max])
    plt.xlabel("Real Time")
    plt.ylabel("Predicted Time")

    # Plot the best fit line over the actual values
    plt.plot(abline_values, abline_values, c="r", linestyle="-")

    output_path = os.path.join(
        out + "plots",
        f"{method}_{encoding_left}_{encoding_right}_{operator}_{implementation}_{left_column_type}_{right_column_type}_{data_type}.png",
    )
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()

    return model_scores


def calculate_error(test_X, y_true, y_pred, model):
    # calculate Root-mean-squared error (RMSE) for the model
    RMSE = mean_squared_error(y_true, y_pred, squared=False)

    # The score function returns the coefficient of determination R^2 of the prediction.
    # It gives a fast notion of how well a model is predicting since the score can be between -1 and 1; 1 being the
    # optimum and 0 meaning the model is as good as just predicting the median of the training data.
    R2 = model.score(test_X, y_pred)

    # logarithm of the accuracy ratio (LnQ)
    # TODO: causes overflow regularly
    # y_true_copy = np.array(y_true, dtype=np.longdouble)
    LNQ = 1  # / len(y_true) * np.sum(np.exp(np.divide(y_pred, y_true_copy)))

    # mean absolute percentage error (MAPE)
    MAPE = np.mean(100 * np.divide(np.abs(y_true - y_pred), y_true))

    return {
        "RMSE": "%.3f" % RMSE,
        "R2": "%.3f" % R2,
        "LNQ": "%.3f" % LNQ,
        "MAPE": "%.3f" % MAPE,
    }


def log(scores, out):
    with open(f"{out}log.txt", "w") as file:
        for entry in scores:
            file.write(f"{entry}: {scores[entry]} \n")


# needed for prediction with one-hot-encoding in case training and test data don't have the same set of values in a
# categorical data column
def add_dummy_types(train, test, cols):
    train["DUMMY"] = "0"
    test["DUMMY"] = "0"
    for col in cols:
        diff1 = np.setdiff1d(
            train[train["DUMMY"] != "Dummy"][col].unique(), test[test["DUMMY"] != "Dummy"][col].unique()
        )
        diff2 = np.setdiff1d(
            test[test["DUMMY"] != "Dummy"][col].unique(), train[train["DUMMY"] != "Dummy"][col].unique()
        )
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


def parse_cost_model_arguments(opt=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-train", help="Directory of training data", metavar="TRAIN_DIR")
    # in case no test data is given, the training data will be split into training and test data
    parser.add_argument(
        "--test", help="Directory of test data. If absent, training data will be split", metavar="TEST_DIR",
    )
    # We have achieved the best results with boost
    parser.add_argument(
        "--m",
        choices={"linear", "lasso", "ridge", "boost"},
        default=["boost"],
        action="append",
        nargs="+",
        help="Model type(s). Boost is the default",
    )
    parser.add_argument(
        "--out", default="cost_model_output", help="Output directory. Default is cost_model_output", metavar="OUT_DIR",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate plots comparing actual and predicted run-times (default: False)",
    )

    if opt:
        return parser.parse_args(opt)
    else:
        return parser.parse_args()


def import_data(train_path, test_path):
    drop_columns_general = ["ESTIMATED_INPUT_ROWS", "ESTIMATED_CARDINALITY"]
    drop_columns_join = [
        "ESTIMATED_INPUT_ROWS_LEFT",
        "ESTIMATED_INPUT_ROWS_RIGHT",
        "ESTIMATED_CARDINALITY",
        "PREDICATE_CONDITION",
    ]
    test_general = import_operator_data(test_path, "operators.csv")
    test_joins = import_operator_data(test_path, "joins.csv")
    test_joins = test_joins.drop(labels=["JOIN_ID"], axis=1)
    test_general = test_general.dropna()
    test_joins = test_joins.dropna()
    test_general = test_general.drop(labels=drop_columns_general, axis=1)
    test_joins = test_joins.drop(labels=drop_columns_join, axis=1)
    test_general = test_general[test_general["OPERATOR_NAME"] != "Aggregate"]

    train_general = import_operator_data(train_path, "operators.csv")
    train_joins = import_operator_data(train_path, "joins.csv")
    train_joins = train_joins.drop(labels=["JOIN_ID"], axis=1)
    train_general = train_general.dropna()
    train_joins = train_joins.dropna()
    train_general = train_general.drop(labels=drop_columns_general, axis=1)
    train_joins = train_joins.drop(labels=drop_columns_join, axis=1)
    train_general = train_general[train_general["OPERATOR_NAME"] != "Aggregate"]

    # check whether training and test data have the same format
    for test, train in zip([test_general, test_joins], [train_general, train_joins]):
        if test.columns.all() != train.columns.all():
            warnings.warn(
                "Warning: Training and Test data do not have the same format. Unmatched columns will be ignored!"
            )

    general_inter = train_general.columns.intersection(test_general.columns)
    join_inter = train_joins.columns.intersection(test_joins.columns)
    test_general = test_general[general_inter.tolist()]
    train_general = train_general[general_inter.tolist()]
    test_joins = test_joins[join_inter.tolist()]
    train_joins = train_joins[join_inter.tolist()]

    return {"general": train_general, "join": train_joins}, {"general": test_general, "join": test_joins}


def equalize_columns(df_left, df_right):
    df_left = df_left.rename(
        columns={
            "MAX_CHUNK_SIZE": "MAX_CHUNK_SIZE_LEFT",
            "DATA_TYPE": "DATA_TYPE_LEFT",
            "ENCODING": "ENCODING_LEFT",
            "SORTED": "LEFT_SORTED",
            "COMPRESSION_TYPE": "COMPRESSION_TYPE_LEFT",
            "INPUT_COLUMNS": "INPUT_COLUMNS_LEFT",
            "INPUT_ROWS": "INPUT_ROWS_LEFT",
        }
    )
    left_exclusive_columns = df_left.columns.difference(df_right.columns)
    right_exclusive_columns = df_right.columns.difference(df_left.columns)
    categorical_column_names = [
        "OPERATOR_NAME",
        "COLUMN_TYPE",
        "DATA_TYPE",
        "ENCODING",
        "OPERATOR_IMPLEMENTATION",
        "COMPRESSION_TYPE",
        "SORTED",
        "JOIN_MODE",
    ]
    left_categorical_columns = np.concatenate(
        [[label for label in left_exclusive_columns if keyword in label] for keyword in categorical_column_names]
    )
    right_categorical_columns = np.concatenate(
        [[label for label in right_exclusive_columns if keyword in label] for keyword in categorical_column_names]
    )
    left_continuous_columns = left_exclusive_columns.difference(left_categorical_columns)
    right_continuous_columns = right_exclusive_columns.difference(right_categorical_columns)

    for col in left_categorical_columns:
        df_right[col] = "0"
    for col in right_categorical_columns:
        df_left[col] = "0"
    for col in left_continuous_columns:
        df_right[col] = 0
    for col in right_continuous_columns:
        df_left[col] = 0

    return df_left, df_right


def train_cost_models(train_data, test_data, args):
    scores = {}
    model_types = args.m if len(args.m) == 1 else args.m[-1]
    out = args.out if args.out.endswith(os.sep) else args.out + os.sep

    if not os.path.exists(out + "models"):
        os.makedirs(out + "models")

    if not os.path.exists(out + "plots"):
        os.makedirs(out + "plots")

    # one single model for everything
    gtrain, gjoin_train = equalize_columns(train_data["general"].copy(), train_data["join"].copy())
    gtest, gjoin_test = equalize_columns(test_data["general"].copy(), test_data["join"].copy())
    gtrain = gtrain.append(gjoin_train, sort=False)
    gtest = gtest.append(gjoin_test, sort=False)

    ohe_candidates = [
        "OPERATOR_NAME",
        "COLUMN_TYPE",
        "DATA_TYPE",
        "ENCODING",
        "OPERATOR_IMPLEMENTATION",
        "COMPRESSION_TYPE",
        "SORTED",
        "JOIN_MODE",
    ]
    dummy_columns = np.concatenate(
        [[label for label in gtrain.columns if keyword in label] for keyword in ohe_candidates]
    )
    """for model_type in model_types:
        gtrain_data, gtest_data = add_dummy_types(
            gtrain.copy(),
            gtest.copy(),
            dummy_columns,
        )
        gtrain_data = preprocess_data(gtrain_data)
        gtest_data = preprocess_data(gtest_data)
        gmodel = train_model(gtrain_data, model_type)
        scores[f"{model_type}_general_model"] = generate_model_plot(
            gmodel, gtest_data, "all", model_type, "all", "all", "all", "all", "all", "all", out
        )
        filename = os.path.join(out + "models", f"{model_type}_general_model.py")
        #joblib.dump(gmodel, filename)
        gmodel.export(filename)
    """

    # make separate models for different operators and combinations of encodings/compressions
    for operator_data in train_data:
        for operator in train_data[operator_data]["OPERATOR_NAME"].unique():
            for encoding_left in train_data[operator_data]["ENCODING_LEFT"].unique():
                for encoding_right in train_data[operator_data]["ENCODING_RIGHT"].unique():
                    for implementation_type in train_data[operator_data]["OPERATOR_IMPLEMENTATION"].unique():
                        for left_column_type in train_data[operator_data]["LEFT_COLUMN_TYPE"].unique():
                            for right_column_type in train_data[operator_data]["RIGHT_COLUMN_TYPE"].unique():
                                data_types = (
                                    train_data[operator_data]["DATA_TYPE_LEFT"].unique()
                                    if operator == "Join"
                                    else train_data[operator_data]["DATA_TYPE"].unique()
                                )
                                for data_type in data_types:
                                    if operator == "Join" and not data_type == "int":
                                        continue
                                    print(
                                        f"{encoding_left}_{encoding_right}_{operator}_{implementation_type}_{left_column_type}_{right_column_type}_{data_type}"
                                    )
                                    # print(f"{model_type}_{encoding_left}_{encoding_right}_{operator}_{implementation_type}")
                                    train_data_copy = train_data[operator_data].copy()
                                    test_data_copy = test_data[operator_data].copy()
                                    model_train_data = train_data_copy.loc[
                                        (train_data_copy["OPERATOR_NAME"] == operator)
                                        & (train_data_copy["ENCODING_LEFT"] == encoding_left)
                                        & (train_data_copy["ENCODING_RIGHT"] == encoding_right)
                                        & (train_data_copy["OPERATOR_IMPLEMENTATION"] == implementation_type)
                                        & (train_data_copy["LEFT_COLUMN_TYPE"] == left_column_type)
                                        & (train_data_copy["RIGHT_COLUMN_TYPE"] == right_column_type)
                                    ]
                                    model_test_data = test_data_copy.loc[
                                        (test_data_copy["OPERATOR_NAME"] == operator)
                                        & (test_data_copy["ENCODING_LEFT"] == encoding_left)
                                        & (test_data_copy["ENCODING_RIGHT"] == encoding_right)
                                        & (test_data_copy["OPERATOR_IMPLEMENTATION"] == implementation_type)
                                        & (test_data_copy["LEFT_COLUMN_TYPE"] == left_column_type)
                                        & (test_data_copy["RIGHT_COLUMN_TYPE"] == right_column_type)
                                    ]

                                    drop_labels = [
                                        "OPERATOR_NAME",
                                        "ENCODING_LEFT",
                                        "ENCODING_RIGHT",
                                        "OPERATOR_IMPLEMENTATION",
                                        "LEFT_COLUMN_TYPE",
                                        "RIGHT_COLUMN_TYPE",
                                    ]

                                    model_train_data = model_train_data.drop(labels=drop_labels, axis=1,)
                                    model_test_data = model_test_data.drop(labels=drop_labels, axis=1,)
                                    if operator == "Join":
                                        model_test_data = model_test_data.loc[
                                            (model_test_data["DATA_TYPE_LEFT"] == data_type)
                                        ]
                                        model_train_data = model_train_data.loc[
                                            (model_train_data["DATA_TYPE_LEFT"] == data_type)
                                        ]

                                        model_train_data = model_train_data.drop(
                                            # labels=["JOIN_MODE", "SELECTIVITY_LEFT", "SELECTIVITY_RIGHT", "DATA_TYPE_LEFT", "DATA_TYPE_RIGHT"], axis=1,
                                            labels=["DATA_TYPE_LEFT", "DATA_TYPE_RIGHT"],
                                            axis=1,
                                        )
                                        model_test_data = model_test_data.drop(
                                            # labels=["JOIN_MODE", "SELECTIVITY_LEFT", "SELECTIVITY_RIGHT", "DATA_TYPE_LEFT", "DATA_TYPE_RIGHT"], axis=1,
                                            labels=["DATA_TYPE_LEFT", "DATA_TYPE_RIGHT"],
                                            axis=1,
                                        )

                                    dummy_columns = np.concatenate(
                                        [
                                            [label for label in model_train_data.columns if keyword in label]
                                            for keyword in ohe_candidates
                                        ]
                                    )
                                    model_train_data, model_test_data = add_dummy_types(
                                        model_train_data.copy(), model_test_data.copy(), dummy_columns,
                                    )
                                    model_train_data = preprocess_data(model_train_data)
                                    model_test_data = preprocess_data(model_test_data)

                                    # if there is training data for this combination, train a model
                                    if not model_train_data.empty:
                                        print("model train != empty")
                                        for model_type in model_types:
                                            model = train_model(model_train_data, model_type)
                                            model_name = f"{model_type}_{encoding_left}_{encoding_right}_{operator}_{implementation_type}_{left_column_type}_{right_column_type}_{data_type}_model"
                                            filename = os.path.join(out + "models", f"split_{model_name}.py")
                                            # joblib.dump(model, filename)
                                            model.export(filename)
                                            if not model_test_data.empty:
                                                print("model test != empty")
                                                scores[model_name] = generate_model_plot(
                                                    model,
                                                    model_test_data,
                                                    operator,
                                                    model_type,
                                                    encoding_left,
                                                    encoding_right,
                                                    implementation_type,
                                                    left_column_type,
                                                    right_column_type,
                                                    data_type,
                                                    out,
                                                )
                                    if not operator == "Join":
                                        break
    log(scores, out)


def equalize_data(data):
    data["join"]["OPERATOR_NAME"] = "Join"
    data["join"] = data["join"].rename(columns={"JOIN_IMPLEMENTATION": "OPERATOR_IMPLEMENTATION"})
    data["general"]["ENCODING_RIGHT"] = "0"
    data["general"] = data["general"].rename(columns={"ENCODING": "ENCODING_LEFT", "COLUMN_TYPE": "LEFT_COLUMN_TYPE"})
    data["general"]["RIGHT_COLUMN_TYPE"] = "0"
    return data


def main(arguments):
    if args.test:
        train_data, test_data = import_data(args.train, args.test)
    else:
        train_data = import_operator_data(args.train)
        train_data = train_data.dropna()
        train_data, test_data = train_test_split(train_data)
        train_general = import_operator_data(train_path, "operators.csv")
        train_joins = import_operator_data(train_path, "joins.csv")
        train_joins = join_data.drop(labels=["JOIN_ID"], axis=1)
        train_general = train_general.dropna()
        train_joins = train_joins.dropna()
        train_general, test_general = train_test_split(train_general)
        train_joins, test_joins = train_test_split(train_joins)
        train_data = {"general": train_general, "join": train_joins}
        test_data = {"general": test_general, "join": test_joins}
    test_data = equalize_data(test_data)
    train_data = equalize_data(train_data)
    train_cost_models(train_data, test_data, arguments)


if __name__ == "__main__":
    arguments = parse_cost_model_arguments()
    main(arguments)
