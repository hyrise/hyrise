#!/usr/bin/python

import pandas as pd
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
        y = df['execution_time_ns']
        x = df.copy()

        if 'execution_time_ms' in df.columns:
            x = x.drop(['execution_time_ms'], axis=1)
        
        if 'execution_time_ns' in df.columns:
            x = x.drop(['execution_time_ns'], axis=1)

        return x, pd.DataFrame(y)

    @staticmethod
    def prepare_df_table_scan(df):
        df['rounded_selectivity'] = df.selectivity.round(1)
        df['is_selectivity_below_50_percent'] = (df.selectivity < 0.5).astype(int)
        df['selectivity_distance_to_50_percent'] = abs(df.selectivity - 0.5)
        df['branch_misprediction_factor'] = -2 * ((df.selectivity - 0.5) ** 2) + 0.5
        df['is_small_table'] = (df.left_input_row_count < 5000).astype(int)

        df['second_column_data_type'] = df.second_column_data_type.fillna('undefined')
        df['third_column_data_type'] = df.third_column_data_type.fillna('undefined')

        df['first_column_is_string_column'] = (df.first_column_data_type == 'string').astype(int)
        df['second_column_is_string_column'] = (df.second_column_data_type == 'string').astype(int)
        df['third_column_is_string_column'] = (df.third_column_data_type == 'string').astype(int)

        df['is_result_empty'] = (df.output_row_count == 0).astype(int)

        encoding_column_to_type = {
            'first_column_segment_encoding_Unencoded_percentage':'Unencoded',
            'first_column_segment_encoding_Dictionary_percentage':'Dictionary',
            'first_column_segment_encoding_RunLength_percentage':'RunLength',
            'first_column_segment_encoding_FixedStringDictionary_percentage':'FixedStringDictionary',
            'first_column_segment_encoding_FrameOfReference_percentage':'FrameOfReference',
            'second_column_segment_encoding_Unencoded_percentage':'Unencoded',
            'second_column_segment_encoding_Dictionary_percentage':'Dictionary',
            'second_column_segment_encoding_RunLength_percentage':'RunLength',
            'second_column_segment_encoding_FixedStringDictionary_percentage':'FixedStringDictionary',
            'second_column_segment_encoding_FrameOfReference_percentage':'FrameOfReference',
            'third_column_segment_encoding_Unencoded_percentage':'Unencoded',
            'third_column_segment_encoding_Dictionary_percentage':'Dictionary',
            'third_column_segment_encoding_RunLength_percentage':'RunLength',
            'third_column_segment_encoding_FixedStringDictionary_percentage':'FixedStringDictionary',
            'third_column_segment_encoding_FrameOfReference_percentage':'FrameOfReference',
        }

        def reverse_one_hot_encoding(df, columns):
            def get_encoding(row):
                for c in columns:
                    #print(row[c])
                    if row[c]==1.0:
                        #print (encoding_column_to_type[c])
                        return encoding_column_to_type[c]
                #print('undefined')
                return 'undefined'
            return df.apply(get_encoding, axis=1)

        df['first_column_segment_encoding'] = reverse_one_hot_encoding(df, [
            'first_column_segment_encoding_Unencoded_percentage',
            'first_column_segment_encoding_Dictionary_percentage',
            'first_column_segment_encoding_RunLength_percentage',
            'first_column_segment_encoding_FixedStringDictionary_percentage',
            'first_column_segment_encoding_FrameOfReference_percentage'
        ])
        #print('Finished reversing one-hot-encoding for first column')

        df['second_column_segment_encoding'] = reverse_one_hot_encoding(df, [
            'second_column_segment_encoding_Unencoded_percentage',
            'second_column_segment_encoding_Dictionary_percentage',
            'second_column_segment_encoding_RunLength_percentage',
            'second_column_segment_encoding_FixedStringDictionary_percentage',
            'second_column_segment_encoding_FrameOfReference_percentage', 
        ])
        #print('Finished reversing one-hot-encoding for second column')

        df['third_column_segment_encoding'] = reverse_one_hot_encoding(df, [
            'third_column_segment_encoding_Unencoded_percentage',
            'third_column_segment_encoding_Dictionary_percentage',
            'third_column_segment_encoding_RunLength_percentage',
            'third_column_segment_encoding_FixedStringDictionary_percentage',
            'third_column_segment_encoding_FrameOfReference_percentage', 
        ])
        #print('Finished reversing one-hot-encoding for third column')

        encoding_categories = ['Unencoded', 'Dictionary', 'RunLength', 'FixedStringDictionary', 'FrameOfReference', 'undefined']
        data_type_categories = ['int', 'long', 'float', 'double', 'string', 'undefined'] #null

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

        df = set_categories(df, 'first_column_data_type', data_type_categories)
        df = set_categories(df, 'second_column_data_type', data_type_categories)
        df = set_categories(df, 'third_column_data_type', data_type_categories)

        df = set_categories(df, 'scan_operator_type', scan_operator_categories)

        df = set_categories(df, 'operator_type', ['IndexScan', 'TableScan'])

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
