import argparse
import joblib
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import warnings

from prepare_calibration_data import import_train_data
from prepare_tpch import import_test_data
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
plt.style.use('ggplot')


def preprocess_data(data):
    # one-hot encoding
    ohe_data = data.drop(labels=['TABLE_NAME', 'COLUMN_NAME'], axis=1)
    ohe_data = pd.get_dummies(ohe_data, columns=['SCAN_TYPE', 'DATA_TYPE', 'ENCODING', 'SCAN_IMPLEMENTATION', 'COMPRESSION_TYPE'])
    return ohe_data


def train_model(train_data, type):
    ohe_data = preprocess_data(train_data)
    y = np.ravel(ohe_data[['RUNTIME_NS']])
    X = ohe_data.drop(labels=['RUNTIME_NS'], axis=1)
    if type == 'linear':
        model = LinearRegression().fit(X, y)
    elif type == 'ridge':
        model = Ridge(alpha=1000).fit(X, y)
    elif type == 'lasso':
        model = Lasso(alpha=1000).fit(X, y)
    elif type == 'boost':
        model = GradientBoostingRegressor(loss='huber').fit(X, y)

    return model


def generate_model_plot(model, test_data, method, data_type, encoding, out):
    ohe_data = preprocess_data(test_data)
    real_y = np.ravel(ohe_data[['RUNTIME_NS']])
    ohe_data = ohe_data.drop(labels=['RUNTIME_NS'], axis=1)
    pred_y = model.predict(ohe_data)

    model_scores = calculate_error(ohe_data, pred_y, real_y, model)

    plt.scatter(real_y, pred_y, c='orange')

    pred_y = model.predict(ohe_data)
    axis_max = max(np.amax(pred_y), np.amax(real_y)) * 1.05
    axis_min = min(np.amin(pred_y), np.amin(real_y)) * 0.95
    abline_values = range(int(axis_min), int(axis_max), int((axis_max-axis_min)/100))

    # Plot the best fit line over the actual values
    plt.plot(abline_values, abline_values, c = 'r', linestyle="-")
    plt.title('{}_{}_{}; Score: {}'.format(data_type, encoding, method, model_scores['R2']))
    plt.ylim([axis_min, axis_max])
    plt.xlim([axis_min, axis_max])
    plt.xlabel("Real Time")
    plt.ylabel("Predicted Time")
    output_path = '{}/Plots/{}_{}_{}'.format(out, method, data_type, encoding)
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()


def calculate_error(test_X, y_true, y_pred, model):
    # calculate error (ME) for the model
    mse = mean_squared_error(y_true, y_pred, squared=False)

    # The score function returns the coefficient of determination R^2 of the prediction.
    # The coefficient R^2 is defined as (1 - u/v), where u is the residual sum of squares ((y_true - y_pred) ** 2).sum()
    # and v is the total sum of squares ((y_true - y_true.mean()) ** 2).sum(). The best possible score is 1.0 and it can
    # be negative (because the model can be arbitrarily worse). A constant model that always predicts the expected value
    # of y, disregarding the input features, would get a R^2 score of 0.0.
    R2 = model.score(test_X, y_pred)

    # Laut Master Thesis:
    # logarithm of the accuracy ratio (LnQ). Tofallis et al. define this measure to overcome the limitations of RMSE and
    # MAPE by providing a unbiased error measure [72]. LnQ is the natural logarithm of the quotient of the predicted
    # value yˆ and the actual value y. By using the natural logarithm, this measure
    # is symmetric in the sense that changing the actual value and the prediction merely changes the sign of the error
    # value (cf. Equation (2.14)). By that, LnQ shows structural under- or over- estimation, which may show the quality
    # of a tested model. A drawback of LnQ is the difficulty in interpreting error values. Doubling the quotient does
    # not consistently affect the error value. Due to the logarithmic function, the measure penalizes small quotients
    # relatively hard, whereas it treats large residuals comparably easy.
    LNQ = 1/len(y_true) * np.sum(np.exp(np.divide(y_pred, y_true)))

    scores = {'MSE': mse, 'R2': R2, 'LNQ': LNQ}
    print(scores)

    return scores


# needed for prediction with one-hot-encoding in case trainings and test data don't have the same set of values in a
# categorical data column
def add_dummy_types(train, test, cols):
    for col in cols:
        diff1 = np.setdiff1d(train[col].unique(), test[col].unique())
        diff2 = np.setdiff1d(test[col].unique(), train[col].unique())
        for d1 in diff1:
            test['{}_{}'.format(col, d1)] = 0
        for d2 in diff2:
            train['{}_{}'.format(col, d2)]= 0
    return [train, test]


def parse_args(opt=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-train', help='Trainingsdata in csv format', action='append', nargs='+')
    # in case no test data is given, the trainings data will be split into trainings and test data
    parser.add_argument('--test', help='Testdata in csv format', action='append', nargs='+')
    parser.add_argument('--m', help='Model type: choose from "linear", "lasso", "ridge", "boost"; Boost is the default"')
    parser.add_argument('--out', help='Output folder')

    if (opt):
        return parser.parse_args(opt)
    else:
        return parser.parse_args()


def import_data(args):
    test_data = import_train_data(args.test[0])
    test_data = test_data.dropna()

    train_data = import_train_data(args.train[0])
    train_data = train_data.dropna()

    # check whether trainings and testdata have the same format
    if test_data.columns.all() != train_data.columns.all():
        warnings.warn("Warning: Trainings- and Testdata do not have the same format. Unmatched columns will be ignored!")

    inter = train_data.columns.intersection(test_data.columns)
    test_data = test_data[inter.tolist()]
    train_data = train_data[inter.tolist()]

    return [train_data, test_data]


def main(args):
    out = "CostModelOutput"

    if args.m:
        model_types = [args.m]
    else:
        model_types = ['boost']

    if args.test:
        train_data, test_data = import_data(args)
    else:
        train_data = import_train_data(args.train[0])
        train_data = train_data.dropna()
        train_data, test_data = train_test_split(train_data)

    if args.out:
        out = args.out

    if not os.path.exists("{}/Models".format(out)):
        os.makedirs("{}/Models".format(out))

    if not os.path.exists("{}/Plots".format(out)):
        os.makedirs("{}/Plots".format(out))

    # one single model for everything
    for type in model_types:
        gtrain_data, gtest_data = add_dummy_types(train_data.copy(), test_data.copy(), ['COMPRESSION_TYPE', 'SCAN_IMPLEMENTATION', 'SCAN_TYPE', 'DATA_TYPE', 'ENCODING'])
        gmodel = train_model(gtrain_data, type)
        generate_model_plot(gmodel, gtest_data, type, 'all', 'all', out)
        filename = '{}/Models/{}_general_model.sav'.format(out, type)
        joblib.dump(gmodel, filename)

    # make separate models for different scan operators and combinations of encodings/compressions
    for encoding in train_data['ENCODING'].unique():
        for data_type in train_data['DATA_TYPE'].unique():

            # if there is no given test data set, split the given trainings data into test and trainings data
            if not args.test:
                model_train_data, model_test_data = train_test_split(train_data.loc[(train_data['DATA_TYPE'] == data_type) &
                                                                                    (train_data['ENCODING'] == encoding)])
            else:
                model_train_data = train_data.loc[(train_data['DATA_TYPE'] == data_type) & (train_data['ENCODING'] == encoding)]
                model_test_data = test_data.loc[(test_data['DATA_TYPE'] == data_type) & (test_data['ENCODING'] == encoding)]

            # if there is training data for this combination, train a model
            if not model_train_data.empty:
                for type in model_types:
                    model_train_data, model_test_data = add_dummy_types(model_train_data.copy(), model_test_data.copy(), ['COMPRESSION_TYPE', 'SCAN_IMPLEMENTATION', 'SCAN_TYPE'])
                    model = train_model(model_train_data, type)

                    filename = '{}/Models/split_{}_{}_{}_model.sav'.format(out, type, data_type, encoding)
                    joblib.dump(model, filename)

                    if not model_test_data.empty:
                        generate_model_plot(model, model_test_data, type, data_type, encoding, out)


if __name__ == '__main__':
    args = parse_args()
    main(args)
