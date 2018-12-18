#!/usr/bin/python

import pandas as pd
from sklearn import preprocessing as pre

import warnings

warnings.filterwarnings('ignore')


class TrainingDataPipeline:

    @staticmethod
    def get_non_categorical_features(X):
        return X.dtypes != 'category'

    @staticmethod
    def get_categorical_features(X):
        return X.dtypes == 'category'

    @staticmethod
    def drop_nan(X):
        return X.dropna(axis='columns')

    @staticmethod
    def extract_features_and_target(df):
        x = df.drop(['execution_time_ms', 'execution_time_ns'], axis=1)
        y = df['execution_time_ns']

        return x, pd.DataFrame(y)

    @staticmethod
    def prepare_df_table_scan(df):
        df['is_output_selectivity_below_50_percent'] = df.output_selectivity < 0.5
        df['output_selectivity_distance_to_50_percent'] = abs(df.output_selectivity - 0.5)
        df['is_small_table'] = df.left_input_row_count < 1000

        encoding_categories = ['Unencoded', 'Dictionary', 'RunLength', 'FixedStringDictionary', 'FrameOfReference', 'undefined']
        boolean_categories = [False, True]
        data_type_categories = ['null', 'int', 'long', 'float', 'double', 'string', 'undefined']

        # And many more... not sure how to cover this
        scan_operator_categories = ['LIKE', 'NOT LIKE', '>','<', '!=', '=', '<=', '>=', 'BETWEEN', 'Or', 'undefined', 'IN']

        df = df[(df['operator_type'] == 'TableScan') | (df['operator_type'] == 'IndexScan')]

        df['first_column_segment_encoding'] = df['first_column_segment_encoding']\
            .astype('category', categories=encoding_categories)
        df['second_column_segment_encoding'] = df['second_column_segment_encoding']\
            .astype('category', categories=encoding_categories)
        df['third_column_segment_encoding'] = df['third_column_segment_encoding']\
            .astype('category', categories=encoding_categories)
        df['is_column_comparison'] = df['is_column_comparison'].astype('category', categories=boolean_categories)

        df['first_column_is_segment_reference_segment'] = df['first_column_is_segment_reference_segment'] \
            .astype('category', categories=boolean_categories)
        df['second_column_is_segment_reference_segment'] = df['second_column_is_segment_reference_segment'] \
            .astype('category', categories=boolean_categories)
        df['third_column_is_segment_reference_segment'] = df['third_column_is_segment_reference_segment'] \
            .astype('category', categories=boolean_categories)

        df['first_column_segment_data_type'] = df['first_column_segment_data_type']\
            .astype('category', categories=data_type_categories)
        df['second_column_segment_data_type'] = df['second_column_segment_data_type'] \
            .astype('category', categories=data_type_categories)
        df['third_column_segment_data_type'] = df['third_column_segment_data_type'] \
            .astype('category', categories=data_type_categories)

        df['scan_operator_type'] = df['scan_operator_type'] \
            .astype('category', categories=scan_operator_categories)

        df['operator_type'] = df['operator_type'] \
            .astype('category', categories=['IndexScan', 'TableScan'])

        df['is_output_selectivity_below_50_percent'] = df['is_output_selectivity_below_50_percent'] \
            .astype('category', categories=boolean_categories)

        df['is_small_table'] = df['is_small_table'] \
            .astype('category', categories=boolean_categories)            

        df['execution_time_ms'] = df['execution_time_ns'].apply(lambda x: x*1e-6)
        #df['output_selectivity_rounded'] = df['output_selectivity'].round(2)

        return df

    @staticmethod
    def load_data_frame_for_table_scan(source, bz2=False):
        if bz2:
            df = pd.read_csv(source, compression='bz2')
        else:
            df = pd.read_csv(source)
        df = TrainingDataPipeline.prepare_df_table_scan(df)
        df = df.drop('operator_description', axis='columns')
        return pd.get_dummies(df).dropna(axis='columns')

    @staticmethod
    def train_model(model, train, test):
        X_train, y_train = TrainingDataPipeline.extract_features_and_target(train)
        X_test, _ = TrainingDataPipeline.extract_features_and_target(test)

        model.fit(X_train, y_train)
        return model.predict(X_test)
