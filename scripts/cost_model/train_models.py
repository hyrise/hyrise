from training_data_pipeline import TrainingDataPipeline
import pandas as pd
from sklearn.base import clone

class ModelTrainer:

    def __init__(self):
        pass


    @staticmethod
    def get_groups():
        return [
            'first_column_data_type',
            'first_column_is_reference_segment',
            'is_small_table',
        ]

    @staticmethod
    def get_selected_features_for_table_scan():
        return [
            'left_input_row_count',
            'is_result_empty',
            'selectivity',
            'execution_time_ns',
            'first_column_segment_encoding',
            'first_column_is_reference_segment',
            'first_column_data_type',
            'second_column_segment_encoding',
            'second_column_is_reference_segment',
            'second_column_data_type',
            'third_column_segment_encoding',
            'third_column_is_reference_segment',
            'third_column_data_type',
            'is_column_comparison',
            'computable_or_column_expression_count',
            'is_selectivity_below_50_percent',
            'selectivity_distance_to_50_percent',
            'is_small_table',
            'execution_time_ms'
        ]


    @staticmethod
    def train_model_for_group(model, train):
        X, y = TrainingDataPipeline.extract_features_and_target(train)
        X = pd.get_dummies(X).dropna(axis='columns')
        X = pd.DataFrame(X)

        model.fit(X, y)
        return model

    @staticmethod
    def train_models(train, model):
        train_dfs = {group: x for group, x in train.groupby(ModelTrainer.get_groups())}
        return {
            group: ModelTrainer.train_model_for_group(clone(model), grouped_df)
            for group, grouped_df in train_dfs.items()
        }

