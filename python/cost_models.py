import pandas as pd
from sklearn.linear_model import LinearRegression
import sys


# store model and metadata about the model in one object (there might be better ways, tbd)
class CostModel:

    def __init__(self, model, scan_type, encoding, compression):
        self.model = model
        self.scan_type = scan_type
        self.encoding = encoding
        self.compression = compression


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

    return joined_data


def import_train_data():
    # use the test_data for now as we assume the same structure
    pass


def train_model(train_data):
    # linear regression for now, try other models later
    # one-hot encoding
    ohe_data = train_data.drop(labels=['TABLE_NAME', 'COLUMN_NAME'], axis=1)
    ohe_data = pd.get_dummies(ohe_data, columns=['SCAN_TYPE', 'DATA_TYPE', 'IS_NULLABLE',
                                                 'ENCODING', 'COMPRESSION'])

    y = ohe_data[['RUNTIME_NS']]
    X = ohe_data.drop(labels=['RUNTIME_NS'], axis=1)
    model = LinearRegression().fit(X, y)

    return model


def predict_cost(model, test_data):
    pass


def main():
    tpch = [arg for arg in sys.argv[1:5]]
    test_data = import_test_data(tpch)
    train_data = test_data
    #train_data = import_train_data()

    # also keep one 'universal' model in case some thing comes up, that we don't have a specific model for?
    u_cost_model = train_model(train_data)

    # make models for different scan operators and combinations of encodings/compressions
    cost_models = []
    for scan_type in train_data['SCAN_TYPE'].unique():
        for encoding in train_data['ENCODING'].unique():
            for compression in train_data['COMPRESSION'].unique():
                model = train_model(train_data.loc[(train_data['SCAN_TYPE'] == scan_type) & (train_data['ENCODING'] == encoding)
                                           & (train_data['COMPRESSION'] == compression)])
                cost_models.append(CostModel(model, scan_type, encoding, compression))

    #testing
    print(cost_models[0].model.coef_)

    #pred = predict_cost(cost_model, test_data)


if __name__ == '__main__':
    main()



