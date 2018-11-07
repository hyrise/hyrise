import pandas as pd

from training_data_pipeline import TrainingDataPipeline


class TPCHEvaluation:

    @staticmethod
    def load_data_for_tpch_index(base_path, index, bz2=False):
        tpch_file_path = base_path + str(index)

        if bz2:
            tpch_df = pd.read_csv(tpch_file_path, compression='bz2')
        else:
            tpch_df = pd.read_csv(tpch_file_path)

        return TrainingDataPipeline.prepare_df_table_scan(tpch_df)
