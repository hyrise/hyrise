#!/usr/bin/env python3

import argparse
import joblib
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import warnings
import math

from random import randrange
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from prepare_calibration_data import import_operator_data, preprocess_joins
from util import (
    save_model_input_columns,
    get_join_model_name,
    get_aggregate_model_name,
    get_table_scan_model_name,
    preprocess_data,
    add_dummy_types,
)


def train_model(train_data, model_type):
    print(f"\t{train_data.shape[0]} training samples")
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


def parse_cost_model_arguments(opt=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-train", help="Directory of training data", metavar="TRAIN_DIR", nargs="+", required=True,
    )
    # in case no test data is given, the training data will be split into training and test data
    parser.add_argument(
        "-test", help="Directory of test data. If absent, training data will be split", metavar="TEST_DIR", nargs="+",
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

    parser.add_argument(
        "--set_random_state", action="store_true", help="Use a fixed random state to split train data if necessary"
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
    data_aggregate = import_operator_data(path, "aggregates.csv")
    data_scans = import_operator_data(path, "scans.csv")
    data_joins = import_operator_data(path, "joins.csv")
    data_joins = data_joins.drop(labels=["JOIN_ID"], axis=1)
    data_aggregate = data_aggregate.drop(labels=drop_columns_general, axis=1)
    data_scans = data_scans.drop(labels=drop_columns_general, axis=1)
    data_joins = data_joins.drop(labels=drop_columns_join, axis=1)
    return data_aggregate, data_scans, preprocess_joins(data_joins)


def import_train_test_data(train_path, test_path):
    train_aggregate, train_scans, train_joins = import_data(train_path)
    test_aggregate, train_scans, test_joins = import_data(test_path)

    # check whether training and test data have the same format
    for test, train in zip([test_aggregate, test_scans, test_joins], [train_aggregate, train_scans, train_joins]):
        if test.columns.all() != train.columns.all():
            warnings.warn(
                "Warning: Training and Test data do not have the same format. Unmatched columns will be ignored!"
            )

    aggregate_inter = train_aggregate.columns.intersection(test_aggregate.columns)
    scan_inter = train_scans.columns.intersection(test_scans.columns)
    join_inter = train_joins.columns.intersection(test_joins.columns)
    test_aggregate = test_aggregate[aggregate_inter.tolist()]
    train_aggregate = train_aggregate[aggregate_inter.tolist()]
    test_scans = test_scans[scan_inter.tolist()]
    train_scans = train_scans[scan_inter.tolist()]
    test_joins = test_joins[join_inter.tolist()]
    train_joins = train_joins[join_inter.tolist()]

    return (
        {"aggregate": train_aggregate, "scan": train_scans, "join": train_joins},
        {"aggregate": test_aggregate, "scan": test_scans, "join": test_joins},
    )


def get_join_scores(model_types, train_data, test_data, implementation, ohe_candidates, out):
    scores = dict()

    train_data = train_data.copy()
    test_data = test_data.copy()

    train_data = train_data.dropna()
    test_data = test_data.dropna()

    CHUNK_SIZE = 65535
    train_data["PROBE_TABLE_PRUNED_CHUNK_RATIO"] = train_data.apply(
        lambda x: x["PROBE_TABLE_PRUNED_CHUNKS"] / (math.ceil(x["PROBE_TABLE_ROW_COUNT"] / CHUNK_SIZE)), axis=1
    )
    train_data["BUILD_TABLE_PRUNED_CHUNK_RATIO"] = train_data.apply(
        lambda x: x["BUILD_TABLE_PRUNED_CHUNKS"] / (math.ceil(x["BUILD_TABLE_ROW_COUNT"] / CHUNK_SIZE)), axis=1
    )
    test_data["PROBE_TABLE_PRUNED_CHUNK_RATIO"] = test_data.apply(
        lambda x: x["PROBE_TABLE_PRUNED_CHUNKS"] / (math.ceil(x["PROBE_TABLE_ROW_COUNT"] / CHUNK_SIZE)), axis=1
    )
    test_data["BUILD_TABLE_PRUNED_CHUNK_RATIO"] = test_data.apply(
        lambda x: x["BUILD_TABLE_PRUNED_CHUNKS"] / (math.ceil(x["BUILD_TABLE_ROW_COUNT"] / CHUNK_SIZE)), axis=1
    )

    for build_column_type in train_data["BUILD_COLUMN_TYPE"].unique():
        for probe_column_type in train_data["PROBE_COLUMN_TYPE"].unique():
            print(" ", build_column_type, probe_column_type)
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
                "BUILD_COLUMN_TYPE",
                "PROBE_COLUMN_TYPE",
                "PROBE_INPUT_COLUMN_SORTED",
                #"PROBE_INPUT_CHUNKS",
                "BUILD_INPUT_COLUMN_SORTED",
                #"BUILD_INPUT_CHUNKS",
                "RUNTIME_NS",
                "JOIN_MODE",
                "PROBE_TABLE_PRUNED_CHUNK_RATIO",
                "BUILD_TABLE_PRUNED_CHUNK_RATIO",
            ]

            model_train_data = model_train_data[keep_labels]
            model_test_data = model_test_data[keep_labels]

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
                    model_name = get_join_model_name(model_type, implementation, build_column_type, probe_column_type)
                    filename = os.path.join(out + "models", f"{model_name}.sav")
                    joblib.dump(model, filename)
                    save_model_input_columns(
                        model_train_data.drop(labels=["RUNTIME_NS"], axis=1),
                        os.path.join(out, "models", f"{model_name}_columns.csv"),
                    )

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
    ].copy()
    model_test_data = test_data.loc[
        (test_data["OPERATOR_NAME"] == "TableScan") & (test_data["OPERATOR_IMPLEMENTATION"] == implementation)
    ].copy()

    model_train_data["NONE_MATCH_RATIO"] = np.where(
        model_train_data.SEGMENTS_SCANNED != 0,
        model_train_data.SHORTCUT_NONE_MATCH / model_train_data.SEGMENTS_SCANNED,
        0,
    )
    model_train_data["ALL_MATCH_RATIO"] = np.where(
        model_train_data.SEGMENTS_SCANNED != 0,
        model_train_data.SHORTCUT_ALL_MATCH / model_train_data.SEGMENTS_SCANNED,
        0,
    )
    model_test_data["NONE_MATCH_RATIO"] = np.where(
        model_test_data.SEGMENTS_SCANNED != 0, model_test_data.SHORTCUT_NONE_MATCH / model_test_data.SEGMENTS_SCANNED, 0
    )
    model_test_data["ALL_MATCH_RATIO"] = np.where(
        model_test_data.SEGMENTS_SCANNED != 0, model_test_data.SHORTCUT_ALL_MATCH / model_test_data.SEGMENTS_SCANNED, 0
    )

    if len(model_train_data["NONE_MATCH_RATIO"].unique()) > 0:
        assert model_train_data["NONE_MATCH_RATIO"].min() >= 0
        assert model_train_data["NONE_MATCH_RATIO"].max() <= 1
    if len(model_train_data["ALL_MATCH_RATIO"].unique()) > 0:
        assert model_train_data["ALL_MATCH_RATIO"].min() >= 0
        assert model_train_data["ALL_MATCH_RATIO"].max() <= 1

    if len(model_test_data["NONE_MATCH_RATIO"].unique()) > 0:
        assert model_test_data["NONE_MATCH_RATIO"].min() >= 0
        assert model_test_data["NONE_MATCH_RATIO"].max() <= 1
    if len(model_test_data["ALL_MATCH_RATIO"].unique()) > 0:
        assert model_test_data["ALL_MATCH_RATIO"].min() >= 0
        assert model_test_data["ALL_MATCH_RATIO"].max() <= 1

    keep_labels = [
        "INPUT_ROWS",
        "OUTPUT_ROWS",
        "SELECTIVITY_LEFT",
        "COLUMN_TYPE",
        "INPUT_COLUMN_SORTED",
        "INPUT_CHUNKS",
        "RUNTIME_NS",
        "NONE_MATCH_RATIO",
        "ALL_MATCH_RATIO",
        "DATA_TYPE",
    ]

    model_train_data = model_train_data[keep_labels]
    model_test_data = model_test_data[keep_labels]
    model_train_data.dropna()
    model_test_data.dropna()

    dummy_columns = np.concatenate(
        [[label for label in model_train_data.columns if keyword in label] for keyword in ohe_candidates]
    )

    model_train_data, model_test_data = add_dummy_types(model_train_data.copy(), model_test_data.copy(), dummy_columns)
    model_train_data = preprocess_data(model_train_data)
    model_test_data = preprocess_data(model_test_data)

    # if there is training data for this combination, train a model
    if not model_train_data.empty:
        for model_type in model_types:
            model = train_model(model_train_data, model_type)
            model_name = get_table_scan_model_name(model_type, implementation)
            filename = os.path.join(out + "models", f"{model_name}.sav")
            joblib.dump(model, filename)
            save_model_input_columns(
                model_train_data.drop(labels=["RUNTIME_NS"], axis=1),
                os.path.join(out, "models", f"{model_name}_columns.csv"),
            )
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
        "COLUMN_TYPE",
        "INPUT_COLUMN_SORTED",
        "INPUT_CHUNKS",
        "GROUP_COLUMNS",
        "AGGREGATE_COLUMNS",
        "IS_COUNT_STAR",
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
            model_name = get_aggregate_model_name(model_type, implementation)
            filename = os.path.join(out + "models", f"{model_name}.sav")
            joblib.dump(model, filename)
            save_model_input_columns(
                model_train_data.drop(labels=["RUNTIME_NS"], axis=1),
                os.path.join(out, "models", f"{model_name}_columns.csv"),
            )
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
        "INPUT_COLUMN_SORTED",
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
                print(operator, implementation_type)
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


def equalize_data(abc):
    abc["join"]["OPERATOR_NAME"] = "Join"
    abc["join"] = abc["join"].rename(columns={"JOIN_IMPLEMENTATION": "OPERATOR_IMPLEMENTATION"})
    return abc


def load_all_directories(directories):
    data = None
    for directory in directories:
        aggregates, scans, joins = import_data(directory)
        if data is None:
            data = {"aggregate": aggregates, "scan": scans, "join": joins}
        else:
            data["aggregate"] = pd.concat([data["aggregate"], aggregates], ignore_index=True)
            data["scan"] = pd.concat([data["scan"], scans], ignore_index=True)
            data["join"] = pd.concat([data["join"], joins], ignore_index=True)
    return data


def main(args):
    if args.test:
        test_data = load_all_directories(args.test)
        train_data = load_all_directories(args.train)
    else:
        random_state = 42 if args.set_random_state else randrange(2 ** 32)
        shared_data = load_all_directories(args.train)
        aggregate_train, aggregate_test = train_test_split(
            shared_data["aggregate"], random_state=random_state, test_size=0.2
        )
        scan_train, scan_test = train_test_split(shared_data["scan"], random_state=random_state, test_size=0.2)
        join_train, join_test = train_test_split(shared_data["join"], random_state=random_state, test_size=0.2)
        test_data = {"aggregate": aggregate_test.copy(), "scan": scan_test.copy(), "join": join_test.copy()}
        train_data = {"aggregate": aggregate_train.copy(), "scan": scan_train.copy(), "join": join_train.copy()}

    test_data = equalize_data(test_data)
    train_data = equalize_data(train_data)
    train_cost_models(train_data, test_data, args)


if __name__ == "__main__":
    plt.style.use("ggplot")
    arguments = parse_cost_model_arguments()
    main(arguments)
