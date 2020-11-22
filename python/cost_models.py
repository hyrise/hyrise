#!/usr/bin/env python3

import argparse
import joblib
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import warnings

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from prepare_calibration_data import import_operator_data, preprocess_joins


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
    ]
    ohe_columns = np.concatenate([[label for label in data.columns if keyword in label] for keyword in ohe_candidates])
    ohe_data = pd.get_dummies(data, columns=ohe_columns)
    ohe_columns = ohe_data.columns
    return remove_dummy_types(ohe_data)


def train_model(train_data, model_type):
    ohe_data = train_data.copy()
    y = np.ravel(ohe_data[["RUNTIME_NS"]])
    X = ohe_data.drop(labels=["RUNTIME_NS"], axis=1)
    # model = TPOTRegressor(verbosity=1, random_state=1337, max_time_mins=10)
    # model.fit(X, y)

    for column in train_data.columns:
        un_v = train_data[column].unique()
        if any([type(x) == str for x in un_v]):
            print(column)

    if model_type == "linear":
        model = LinearRegression().fit(X, y)
    elif model_type == "ridge":
        model = Ridge(alpha=10).fit(X, y)
    elif model_type == "lasso":
        model = Lasso(alpha=10).fit(X, y)
    elif model_type == "boost":
        model = GradientBoostingRegressor(loss="huber").fit(X, y)
    # model = LinearRegression().fit(X, y)

    return model


def generate_model_plot(model, test_data, operator, method, implementation, further_args, out):
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
    plt.title(f'{operator} {implementation} {method}; Score: {model_scores["R2"]} ({sample_size} samples)')
    plt.ylim([axis_min, axis_max])
    plt.xlim([axis_min, axis_max])
    plt.xlabel("Real Time")
    plt.ylabel("Predicted Time")

    # Plot the best fit line over the actual values
    plt.plot(abline_values, abline_values, c="r", linestyle="-")
    further_args_str = "_".join(further_args)
    further_args_str = "_" + further_args_str if further_args_str else ""
    output_path = os.path.join(out + "plots", f"{method}_{operator}_{implementation}{further_args_str}.png")
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


def parse_cost_model_arguments(opt=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-train", help="Directory of training data", metavar="TRAIN_DIR", required=True, nargs="+",
    )
    # in case no test data is given, the training data will be split into training and test data
    parser.add_argument(
        "-test",
        help="Directory of test data. If absent, training data will be split",
        metavar="TEST_DIR",
        required=True,
        nargs="+",
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


def import_data(path):
    drop_columns_general = ["ESTIMATED_INPUT_ROWS", "ESTIMATED_CARDINALITY"]
    drop_columns_join = [
        "ESTIMATED_INPUT_ROWS_LEFT",
        "ESTIMATED_INPUT_ROWS_RIGHT",
        "ESTIMATED_CARDINALITY",
    ]
    data_general = import_operator_data(path, "operators.csv")
    data_joins = import_operator_data(path, "joins.csv")
    data_joins = data_joins.drop(labels=["JOIN_ID"], axis=1)
    data_general = data_general.drop(labels=drop_columns_general, axis=1)
    data_joins = data_joins.drop(labels=drop_columns_join, axis=1)
    return data_general, preprocess_joins(data_joins)


def import_train_test_data(train_path, test_path):
    train_general, train_joins = import_data(train_path)
    test_general, test_joins = import_data(test_path)

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

    return (
        {"general": train_general, "join": train_joins},
        {"general": test_general, "join": test_joins},
    )


def get_join_scores(model_types, train_data, test_data, implementation, ohe_candidates, out):
    scores = dict()
    for build_column_type in train_data["BUILD_COLUMN_TYPE"].unique():
        for probe_column_type in train_data["PROBE_COLUMN_TYPE"].unique():
            # for join_mode in train_data["JOIN_MODE"].unique():
            model_train_data = train_data.loc[
                (train_data["OPERATOR_NAME"] == "Join")
                & (train_data["OPERATOR_IMPLEMENTATION"] == implementation)
                & (train_data["BUILD_COLUMN_TYPE"] == build_column_type)
                & (train_data["PROBE_COLUMN_TYPE"] == probe_column_type)
                # & (train_data["JOIN_MODE"] == join_mode)
            ]
            model_test_data = test_data.loc[
                (test_data["OPERATOR_NAME"] == "Join")
                & (test_data["OPERATOR_IMPLEMENTATION"] == implementation)
                & (test_data["BUILD_COLUMN_TYPE"] == build_column_type)
                & (test_data["PROBE_COLUMN_TYPE"] == probe_column_type)
                # & (test_data["JOIN_MODE"] == join_mode)
            ]

            keep_labels = [
                "PROBE_INPUT_ROWS",
                "BUILD_INPUT_ROWS",
                "OUTPUT_ROWS",
                "BUILD_SORTED",
                "PROBE_SORTED",
                "BUILD_COLUMN_TYPE",
                "PROBE_COLUMN_TYPE",
                "RUNTIME_NS",
                "JOIN_MODE",
            ]

            model_train_data = model_train_data[keep_labels]
            model_test_data = model_test_data[keep_labels]
            model_train_data.dropna()
            model_test_data.dropna()

            dummy_columns = np.concatenate(
                [[label for label in model_train_data.columns if keyword in label] for keyword in ohe_candidates]
            )
            model_train_data, model_test_data = add_dummy_types(
                model_train_data.copy(), model_test_data.copy(), dummy_columns,
            )
            model_train_data = preprocess_data(model_train_data)
            model_test_data = preprocess_data(model_test_data)

            # if there is training data for this combination, train a model
            if not model_train_data.empty:
                for model_type in model_types:
                    model = train_model(model_train_data, model_type)
                    model_name = f"{model_type}_Join_{implementation}_{build_column_type}_{probe_column_type}_model"
                    filename = os.path.join(out + "models", f"{model_name}.sav")
                    joblib.dump(model, filename)
                    if not model_test_data.empty:
                        scores[model_name] = generate_model_plot(
                            model,
                            model_test_data,
                            "Join",
                            model_type,
                            implementation,
                            [build_column_type, probe_column_type],
                            out,
                        )
    return scores


def get_table_scan_scores(model_types, train_data, test_data, implementation, ohe_candidates, out):
    scores = dict()
    model_train_data = train_data.loc[
        (train_data["OPERATOR_NAME"] == "TableScan") & (train_data["OPERATOR_IMPLEMENTATION"] == implementation)
    ]
    model_test_data = test_data.loc[
        (test_data["OPERATOR_NAME"] == "TableScan") & (test_data["OPERATOR_IMPLEMENTATION"] == implementation)
    ]

    keep_labels = [
        "INPUT_ROWS",
        "OUTPUT_ROWS",
        "SELECTIVITY_LEFT",
        "COLUMN_TYPE",
        "RUNTIME_NS",
    ]

    model_train_data = model_train_data[keep_labels]
    model_test_data = model_test_data[keep_labels]

    model_train_data.dropna()
    model_test_data.dropna()

    dummy_columns = np.concatenate(
        [[label for label in model_train_data.columns if keyword in label] for keyword in ohe_candidates]
    )
    model_train_data, model_test_data = add_dummy_types(model_train_data.copy(), model_test_data.copy(), dummy_columns,)
    model_train_data = preprocess_data(model_train_data)
    model_test_data = preprocess_data(model_test_data)

    # if there is training data for this combination, train a model
    if not model_train_data.empty:
        for model_type in model_types:
            model = train_model(model_train_data, model_type)
            model_name = f"{model_type}_TableScan_{implementation}_model"
            filename = os.path.join(out + "models", f"{model_name}.sav")
            joblib.dump(model, filename)
            if not model_test_data.empty:
                scores[model_name] = generate_model_plot(
                    model, model_test_data, "TableScan", model_type, implementation, [], out,
                )
    return scores


def get_aggregate_scores(model_types, train_data, test_data, implementation, ohe_candidates, out):
    scores = dict()
    model_train_data = train_data.loc[
        (train_data["OPERATOR_NAME"] == "Aggregate") & (train_data["OPERATOR_IMPLEMENTATION"] == implementation)
    ]
    model_test_data = test_data.loc[
        (test_data["OPERATOR_NAME"] == "Aggregate") & (test_data["OPERATOR_IMPLEMENTATION"] == implementation)
    ]

    keep_labels = [
        "INPUT_ROWS",
        "OUTPUT_ROWS",
        "SELECTIVITY_LEFT",
        "COLUMN_TYPE",
        "RUNTIME_NS",
    ]

    model_train_data = model_train_data[keep_labels]
    model_test_data = model_test_data[keep_labels]

    model_train_data.dropna()
    model_test_data.dropna()

    dummy_columns = np.concatenate(
        [[label for label in model_train_data.columns if keyword in label] for keyword in ohe_candidates]
    )
    model_train_data, model_test_data = add_dummy_types(model_train_data.copy(), model_test_data.copy(), dummy_columns,)
    model_train_data = preprocess_data(model_train_data)
    model_test_data = preprocess_data(model_test_data)

    # if there is training data for this combination, train a model
    if not model_train_data.empty:
        for model_type in model_types:
            model = train_model(model_train_data, model_type)
            model_name = f"{model_type}_Aggregate_{implementation}_model"
            filename = os.path.join(out + "models", f"{model_name}.sav")
            joblib.dump(model, filename)
            if not model_test_data.empty:
                scores[model_name] = generate_model_plot(
                    model, model_test_data, "Aggregate", model_type, implementation, [], out,
                )
    return scores


def train_cost_models(train_data, test_data, args):
    scores = {}
    model_types = args.m if len(args.m) == 1 else args.m[-1]
    out = args.out if args.out.endswith(os.sep) else args.out + os.sep

    if not os.path.exists(out + "models"):
        os.makedirs(out + "models")

    if not os.path.exists(out + "plots"):
        os.makedirs(out + "plots")

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

    # make separate models for different operators
    for operator_data in train_data:
        op_data = train_data[operator_data]
        model_test_data = test_data[operator_data]

        for operator in op_data["OPERATOR_NAME"].unique():
            for implementation_type in op_data[op_data.OPERATOR_NAME.eq(operator)]["OPERATOR_IMPLEMENTATION"].unique():
                if operator == "TableScan":
                    operator_scores = get_table_scan_scores(
                        model_types, op_data.copy(), model_test_data.copy(), implementation_type, ohe_candidates, out,
                    )
                elif operator == "Aggregate":
                    operator_scores = get_aggregate_scores(
                        model_types, op_data.copy(), model_test_data.copy(), implementation_type, ohe_candidates, out,
                    )
                elif operator == "Join":
                    operator_scores = get_join_scores(
                        model_types, op_data.copy(), model_test_data.copy(), implementation_type, ohe_candidates, out,
                    )
                else:
                    continue
                scores.update(operator_scores)
    log(scores, out)


def equalize_data(data):
    data["join"]["OPERATOR_NAME"] = "Join"
    data["join"] = data["join"].rename(columns={"JOIN_IMPLEMENTATION": "OPERATOR_IMPLEMENTATION"})
    return data


def main(args):
    train_data = test_data = None
    for directory in args.train:
        operators, joins = import_data(directory)
        if train_data is None:
            train_data = {"general": operators, "join": joins}
        else:
            train_data["general"] = pd.concat([train_data["general"], operators], ignore_index=True)
            train_data["join"] = pd.concat([train_data["join"], joins], ignore_index=True)
    for directory in args.test:
        operators, joins = import_data(directory)
        if test_data is None:
            test_data = {"general": operators, "join": joins}
        else:
            test_data["general"] = pd.concat([test_data["general"], operators], ignore_index=True)
            test_data["join"] = pd.concat([test_data["join"], joins], ignore_index=True)

    test_data = equalize_data(test_data)
    train_data = equalize_data(train_data)
    train_cost_models(train_data, test_data, args)


if __name__ == "__main__":
    plt.style.use("ggplot")
    arguments = parse_cost_model_arguments()
    main(arguments)
