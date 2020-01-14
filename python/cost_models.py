import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import sys
import matplotlib.pyplot as plt
import joblib


# store model and metadata about the model in one object (there might be better ways, tbd)
class CostModel:

    def __init__(self, model, encoding, data_type):
        self.model = model
        self.encoding = encoding
        self.data_type = data_type


def import_test_data(tpch):
    # read data
    table_scans = pd.read_csv(tpch[0])
    table_meta_data = pd.read_csv(tpch[1])
    segment_meta_data = pd.read_csv(tpch[2])
    attribute_meta_data = pd.read_csv(tpch[3])

    # preprocess data
    joined_data = table_scans.merge(table_meta_data, on="TABLE_NAME", how="left")
    joined_data = joined_data.merge(attribute_meta_data, on="COLUMN_NAME", how="left")
    joined_data = joined_data.merge(segment_meta_data, on="COLUMN_NAME", how="left")

    joined_data = joined_data[['SCAN_TYPE', 'TABLE_NAME', 'COLUMN_NAME', 'INPUT_ROWS',
                               'OUTPUT_ROWS', 'RUNTIME_NS', 'ROW_COUNT_x',
                               'MAX_CHUNK_SIZE', 'DATA_TYPE',
                               'DISTINCT_VALUE_COUNT', 'IS_NULLABLE',
                               'ENCODING', 'COMPRESSION', 'SIZE_IN_BYTES']]
    joined_data = joined_data.rename(columns={"TABLE_NAME_x": "TABLE_NAME", "ROW_COUNT_x": "ROW_COUNT"})
    joined_data['SELECTIVITY'] = (joined_data['OUTPUT_ROWS'] / joined_data['INPUT_ROWS'])

    return joined_data


def import_train_data(input_data):
    table_scan_data = pd.read_csv(input_data[0])
    columns_data = pd.read_csv(input_data[1])
    table_data = pd.read_csv(input_data[2])

    joined_data = table_scan_data.merge(table_data, on=["TABLE_NAME", "COLUMN_NAME"], how="left")

    # extract encoding_type from column_name
    # encodings = [[encoding.split('_')[1]] for encoding in columns_data[['COLUMN_NAME']]]
    # columns_data['ENCODING'] = encodings

    joined_data = joined_data.merge(columns_data, on="TABLE_NAME", how="left")
    joined_data = joined_data.rename(columns={"INPUT_ROWS_LEFT": "INPUT_ROWS", "CHUNK_SIZE": "MAX_CHUNK_SIZE",
                                              "COLUMN_DATA_TYPE": "DATA_TYPE", "ENCODING_TYPE": "ENCODING",
                                              "TABLE_NAME": "TABLE_NAME", "COLUMN_NAME": "COLUMN_NAME",
                                              "COLUMN_DATA_TYPE": "DATA_TYPE", "COLUMN_TYPE": "SCAN_TYPE"})
    joined_data['SELECTIVITY'] = (joined_data['OUTPUT_ROWS'] / joined_data['INPUT_ROWS'])

    return joined_data


def preprocess_data(data):
    # linear regression for now, try other models later
    # one-hot encoding
    ohe_data = data.drop(labels=['TABLE_NAME', 'COLUMN_NAME'], axis=1)
    ohe_data = pd.get_dummies(ohe_data, columns=['SCAN_TYPE', 'DATA_TYPE', 'IS_NULLABLE',
                                                 'ENCODING', 'COMPRESSION'])

    return ohe_data


def train_model(train_data):
    # not for now, since there are no compression types in the data
    #ohe_data = preprocess_data(train_data)

    ohe_data = train_data.drop(labels=['TABLE_NAME', 'COLUMN_NAME'], axis=1)
    ohe_data = pd.get_dummies(ohe_data, columns=['SCAN_TYPE', 'DATA_TYPE', 'ENCODING'])
    print('train ' + ohe_data.columns)
    y = ohe_data[['RUNTIME_NS']]
    X = ohe_data.drop(labels=['RUNTIME_NS'], axis=1)
    model = LinearRegression().fit(X, y)

    return model


def generate_output(costmodel, test_data, out):
    # predict runtime for test queries in the test data
    #ohe_data = preprocess_data(test_data)
    # for now, since we don't have the data in our trainings measurements
    ohe_data = test_data.drop(labels=['TABLE_NAME', 'COLUMN_NAME', 'COMPRESSION', 'SIZE_IN_BYTES',
                                      'DISTINCT_VALUE_COUNT', 'IS_NULLABLE'], axis=1)
    ohe_data = pd.get_dummies(ohe_data, columns=['SCAN_TYPE', 'DATA_TYPE', 'ENCODING'])
    # hacky solution
    ohe_data['DATA_TYPE_long'] = 0
    ohe_data['DATA_TYPE_string'] = 0
    ohe_data['DATA_TYPE_double'] = 0
    print('predict ' + ohe_data.columns)
    y_true = ohe_data[['RUNTIME_NS']]
    ohe_data = ohe_data.drop(labels=['RUNTIME_NS'], axis=1)
    y_pred = costmodel.model.predict(ohe_data)
    error = abs(y_true.to_numpy()-y_pred)
    mse = calculate_error(y_true, y_pred)

    # print and put the plots in the output folder
    fig = plt.figure()
    plt.boxplot(error)
    output_path = '{}error_model_{}_{}'.format(out, costmodel.data_type, costmodel.encoding)
    fig.savefig(output_path, bbox_inches='tight')

    f = plt.figure()
    #plt.scatter(scatter_data)
    plt.show()
    return mse


def calculate_error(y_true, y_pred):
    # calculate error (ME) for the model
    mse = mean_squared_error(y_true, y_pred, squared=False)
    return mse


def plot_predictions(data_true, data_pred, out):
    # plot the true values against the predicted values to visualize the model's accuracies
    fig = plt.figure()
    # plot here

    return fig


def main():
    tpch = [arg for arg in sys.argv[1:5]]
    test_data = import_test_data(tpch)
    train_data = import_train_data([arg for arg in sys.argv[5:8]])
    # TODO: maybe order the test and trainings data in the same way (does it influence the regression?)
    output_folder = sys.argv[8]
    cost_models = []

    # also keep one 'universal' model in case some thing comes up, that we don't have a specific model for?
    u_cost_model = train_model(train_data)
    cost_models.append(CostModel(u_cost_model, data_type='None', encoding='None'))
    # make models for different scan operators and combinations of encodings/compressions
    test_data_splitted = []
    leftover_test_data = []
    for encoding in train_data['ENCODING'].unique():
        for data_type in train_data['DATA_TYPE'].unique():

            model_train_data = train_data.loc[(train_data['DATA_TYPE'] == data_type) & (train_data['ENCODING'] == encoding)]
            model_test_data = test_data.loc[(test_data['DATA_TYPE'] == data_type) & (test_data['ENCODING'] == encoding)]
            # if we have train data for this combination, train a model

            if not model_train_data.empty:
                model = train_model(model_train_data)

                cost_model = CostModel(model, encoding, data_type)
                cost_models.append(cost_model)
                filename = 'models/{}_{}_model.sav'.format(data_type, encoding)
                joblib.dump(model, filename)

                # if there is no test_data for this combination, continue
                if model_test_data.empty:
                    continue
                test_data_splitted.append(model_test_data)

                # predict the runtime for the queries of the test_data that correspond to this model and calculate the
                # error (SSE)
                #model_mse = generate_output(cost_model, model_test_data, output_folder)

            # if we don't have a model for this combination but test_data, add the test_data to test on the universal model
            if not model_test_data.empty:
                leftover_test_data.append(model_test_data)


    # umodel_mse = generate_output(u_cost_model, leftover_test_data, output_folder)

    # TODO: catch the queries that don't correspond to any model and use the 'universal model' for runtime prediction of those

    # TODO: what kind of output is expected? Also: Error as attribute of the CostModel class?

    # TBD mode, parameter: scale data? Use regularization?


if __name__ == '__main__':
    main()
