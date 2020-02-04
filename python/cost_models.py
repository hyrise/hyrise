import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import GradientBoostingRegressor
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import joblib
import numpy as np
import argparse
from prepare_calibration_data import import_train_data
from prepare_tpch import import_test_data
import os

# store model and metadata about the model in one object (there might be better ways, tbd)
class CostModel:

    def __init__(self, model, encoding, data_type):
        self.model = model
        self.encoding = encoding
        self.data_type = data_type


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


#generate_model_plot(model, model_test_data, type, data_type, encoding, out)
def generate_model_plot(model, test_data, method, data_type, encoding, out):
    ohe_data = preprocess_data(test_data)
    real_y = np.ravel(ohe_data[['RUNTIME_NS']])
    ohe_data = ohe_data.drop(labels=['RUNTIME_NS'], axis=1)
    pred_y = model.predict(ohe_data)

    model_scores = calculate_error(ohe_data, pred_y, real_y, model)

    plt.scatter(real_y, pred_y, c='b')
    axis_max = max(np.amax(pred_y), np.amax(real_y)) * 1.05
    axis_min = min(np.amin(pred_y), np.amin(real_y))
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


def train_general_model(train_data, model_type, out):
    train_X = train_data[['INPUT_ROWS', 'OUTPUT_ROWS']]
    train_y = np.ravel(train_data[['RUNTIME_NS']])

    if model_type == 'linear':
        general_model = LinearRegression().fit(train_X, train_y)
    elif model_type == 'ridge':
        general_model = Ridge(alpha=1000).fit(train_X, train_y)
    elif model_type == 'lasso':
        general_model = Lasso(alpha=1000).fit(train_X, train_y)
    elif model_type == 'boost':
        general_model = GradientBoostingRegressor(loss='huber').fit(train_X, train_y)

    # if model_type != 'boost':
    #     print('general', general_model.coef_)

    filename = '{}/Models/{}_general_model.sav'.format(out, model_type)
    joblib.dump(general_model, filename)

    return general_model


def plot_general_model(test_data, model, model_type, data_type, encoding, out):
    test_X = test_data[['INPUT_ROWS', 'OUTPUT_ROWS']]
    test_y = np.ravel(test_data[['RUNTIME_NS']])

    pred_y = model.predict(test_X)
    model_scores = calculate_error(test_X, pred_y, test_y, model)
    plt.scatter(test_y, pred_y, c='b')
    axis_max = max(np.amax(pred_y), np.amax(test_y)) * 1.05
    axis_min = min(0, np.amin(pred_y), np.amin(test_y))
    abline_values = range(int(axis_min), int(axis_max), int((axis_max-axis_min)/100))

    # Plot the best fit line over the actual values
    plt.plot(abline_values, abline_values, c = 'r', linestyle="-")

    plt.title('General Model; Score: {}'.format(model_scores['R2']))
    plt.ylim([0, max(np.amax(pred_y), np.amax(test_y)) * 1.05])
    plt.xlim([0, max(np.amax(pred_y), np.amax(test_y)) * 1.05])
    plt.xlabel("Real Time")
    plt.ylabel("Predicted Time")
    output_path = '{}/Plots/{}_{}_{}_general'.format(out, model_type, data_type, encoding)
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()

    #return model


def add_dummy_types(train, test, cols):
    for col in cols:
        diff1 = np.setdiff1d(train[col].unique(), test[col].unique())
        diff2 = np.setdiff1d(test[col].unique(), train[col].unique())
        for d1 in diff1:
            test['{}_{}'.format(col, d1)] = 0
        for d2 in diff2:
            train['{}_{}'.format(col, d2)]= 0
    return [train, test]


def parseargs(opt=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-train', help='Trainingsdata in csv format', action='append', nargs='+')
    # in case no test data is given, just split the train data in to train and test dataset
    parser.add_argument('--test', help='Testdata in csv format', action='append', nargs='+')
    parser.add_argument('--m', help='Model type')
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
    # TBD: should it rather fail otherwise?
    inter = train_data.columns.intersection(test_data.columns)
    test_data = test_data[inter.tolist()]
    train_data = train_data[inter.tolist()]

    return [train_data, test_data]


def main(args):
    cost_models = []
    test_data_split = []
    leftover_test_data = []
    out = "Output"

    if args.m:
        model_types = [args.m]
    else:
        model_types = ['linear', 'lasso', 'ridge', 'boost']

    if args.test:
        train_data, test_data = import_data(args)
    else:
        train_data = import_train_data(args.train[0])
        train_data = train_data.dropna()

    if args.out:
        out = args.out

    if not os.path.exists("{}/Models".format(out)):
        os.makedirs("{}/Models".format(out))

    if not os.path.exists("{}/Plots".format(out)):
        os.makedirs("{}/Plots".format(out))

    for type in model_types:
        train_general_model(train_data, type, out)

    # make models for different scan operators and combinations of encodings/compressions
    for encoding in train_data['ENCODING'].unique():
        for data_type in train_data['DATA_TYPE'].unique():

            if not args.test:
                model_train_data, model_test_data = train_test_split(train_data.loc[(train_data['DATA_TYPE'] == data_type) &
                                                                                    (train_data['ENCODING'] == encoding)])
            else:
                model_train_data = train_data.loc[(train_data['DATA_TYPE'] == data_type) & (train_data['ENCODING'] == encoding)]
                model_test_data = test_data.loc[(test_data['DATA_TYPE'] == data_type) & (test_data['ENCODING'] == encoding)]

            # TODO: needed to save this?
            #dfilename = 'data/split_{}_{}_train_data.sav'.format(data_type, encoding)
            #joblib.dump(model_test_data, dfilename)

            # if there is train data for this combination, train a model
            if not model_train_data.empty:
                for type in model_types:
                    model_train_data, model_test_data = add_dummy_types(model_train_data, model_test_data, ['COMPRESSION_TYPE', 'SCAN_IMPLEMENTATION', 'SCAN_TYPE'])
                    model = train_model(model_train_data, type)

                    # TODO: needed?
                    cost_model = CostModel(model, encoding, data_type)
                    cost_models.append(cost_model)

                    filename = '{}/Models/split_{}_{}_{}_model.sav'.format(out, type, data_type, encoding)
                    joblib.dump(model, filename)

                    if not model_test_data.empty:
                        # TODO:  nicer plots
                        generate_model_plot(model, model_test_data, type, data_type, encoding, out)
                        general_model = joblib.load('{}/Models/{}_general_model.sav'.format(out, type))
                        plot_general_model(model_test_data, general_model, type, data_type, encoding, out)

                    # test_data_split.append(model_test_data)

            # if we don't have a model for this combination but test_data, add the test_data to test on the
            #  universal model
            if not model_test_data.empty:
                leftover_test_data.append(model_test_data)

    # u_model = make_general_model(train_data, leftover_test_data)
    # u_cost_model = CostModel(u_model, 'all', 'all')
    # cost_models.append(u_cost_model)

    #TODO make general model with only input and output rows against time

    # TODO: catch the queries that don't correspond to any model and use the 'general model' for runtime prediction
    #  of those


if __name__ == '__main__':
    args = parseargs()
    main(args)
