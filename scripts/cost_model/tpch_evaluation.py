from scripts.cost_model.training_data_pipeline import TrainingDataPipeline


class TPCHEvaluation:

    @staticmethod
    def load_data_for_tpch_index(basepath, index, bz2=False):
        tpch_file_path = basepath+str(index)
        return TrainingDataPipeline.prepare_df_table_scan(tpch_file_path, bz2=bz2)
