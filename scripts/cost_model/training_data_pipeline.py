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

        def set_categories(internal_df, column_name, categories):
            if column_name in internal_df.columns:
                internal_df[column_name] = internal_df[column_name].astype('category', categories=categories)

            return internal_df

        df = set_categories(df, 'first_column_segment_encoding', encoding_categories)
        df = set_categories(df, 'second_column_segment_encoding', encoding_categories)
        df = set_categories(df, 'third_column_segment_encoding', encoding_categories)

        df = set_categories(df, 'is_column_comparison', boolean_categories)

        df = set_categories(df, 'first_column_is_segment_reference_segment', boolean_categories)
        df = set_categories(df, 'second_column_is_segment_reference_segment', boolean_categories)
        df = set_categories(df, 'third_column_is_segment_reference_segment', boolean_categories)

        df = set_categories(df, 'first_column_segment_data_type', data_type_categories)
        df = set_categories(df, 'second_column_segment_data_type', data_type_categories)
        df = set_categories(df, 'third_column_segment_data_type', data_type_categories)

        df = set_categories(df, 'scan_operator_type', scan_operator_categories)

        df = set_categories(df, 'operator_type', ['IndexScan', 'TableScan'])

        df = set_categories(df, 'is_output_selectivity_below_50_percent', boolean_categories)

        df = set_categories(df, 'is_small_table', boolean_categories)

        df['execution_time_ms'] = df['execution_time_ns'].apply(lambda x: x*1e-6)

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
