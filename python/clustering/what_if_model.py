import numpy as np
import pandas as pd

from .disjoint_clusters_model import DisjointClustersModel
import util


class WhatIfModel(DisjointClustersModel):
    
    def __init__(self, max_dimensions, query_frequencies, table_name, table_scans, table_sizes, distinct_values, target_chunksize, correlations, joins, sorted_columns_during_creation, model_dir='unfair_training/models/', model_type='boost'):
        super().__init__(max_dimensions, query_frequencies, table_name, table_scans, table_sizes, distinct_values, target_chunksize, correlations, joins, sorted_columns_during_creation)
        self.models = util.load_models(model_dir)
        self.model_formats = util.load_model_input_formats(model_dir)
        self.model_type = model_type
    
    
    def estimate_table_scan_runtime(self, clustering_columns, sorting_column, dimension_cardinalities):        
        runtime = 0
        self.table_scans = self.table_scans.copy()
        self.table_scans['OPERATOR_IMPLEMENTATION'] = self.table_scans.apply(lambda x: x['DESCRIPTION'].split("Impl: ")[1].split()[0], axis=1)
        self.table_scans['RUNTIME_ESTIMATE'] = -1
        
        scans_by_implementation = self.table_scans.groupby(['OPERATOR_IMPLEMENTATION'])
        for operator_implementation, df in scans_by_implementation:
            print(f"## Estimating {operator_implementation} scans")
            model_name = util.get_table_scan_model_name(self.model_type, operator_implementation)
            model = self.models[model_name]
            
            df = df.copy()                        
            df = df.rename(columns={
                'selectivity': 'SELECTIVITY_LEFT',
                'INPUT_ROW_COUNT': 'INPUT_ROWS',
                'OUTPUT_ROW_COUNT': 'OUTPUT_ROWS',
                'PREDICATE_CONDITION': 'PREDICATE',
                'INPUT_CHUNK_COUNT': 'INPUT_CHUNKS',
            })
            
            df['INPUT_COLUMN_SORTED'] = df.apply(lambda x: "Ascending" if x['COLUMN_NAME'] == sorting_column else "No", axis=1)
            
            df = df.drop(columns=['RUNTIME_ESTIMATE', 'COLUMN_NAME', 'DESCRIPTION', 'GET_TABLE_HASH', 'LEFT_INPUT_OPERATOR_HASH', 'OPERATOR_HASH', 'OPERATOR_TYPE', 'OUTPUT_CHUNK_COUNT', 'QUERY_HASH', 'RIGHT_INPUT_OPERATOR_HASH', 'RUNTIME_NS', 'SCANS_SKIPPED', 'SCANS_SORTED', 'TABLE_NAME', 'benefits_from_sorting', 'part_of_or_chain', 'time_per_input_row', 'time_per_output_row', 'time_per_row', 'useful_for_pruning', 'OPERATOR_IMPLEMENTATION'])
            df = util.preprocess_data(df)
            df = util.append_to_input_format(df, self.model_formats[model_name])
            #df = df.drop(columns=['RUNTIME_NS'])
            
            predictions = model.predict(df)
            self.scan_estimates.loc[df.index, 'RUNTIME_ESTIMATE'] = np.array(predictions, dtype=np.int64)
            
            runtime += predictions.sum()
        print()
            
        return runtime
        
    def estimate_join_runtime(self, clustering_columns, sorting_column, dimension_cardinalities):
        runtime = 0
        
        joins = self.joins.copy()
        joins['OPERATOR_IMPLEMENTATION'] = joins.apply(lambda x: x['DESCRIPTION'].split()[0], axis=1)
        
        joins['BUILD_COLUMN_TYPE'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_COLUMN_TYPE"], axis=1)
        joins['BUILD_INPUT_CHUNKS'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_TABLE_CHUNK_COUNT"], axis=1)
        joins['PROBE_COLUMN_TYPE'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_COLUMN_TYPE"], axis=1)
        joins['PROBE_INPUT_CHUNKS'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_TABLE_CHUNK_COUNT"], axis=1)
        
        joins_by_implementation = joins.groupby(['OPERATOR_IMPLEMENTATION', 'BUILD_COLUMN_TYPE', 'PROBE_COLUMN_TYPE'])
        for (implementation, build_type, probe_type), df in joins_by_implementation:
            print(f"## Estimating {build_type} {probe_type} joins")
            model_name = util.get_join_model_name(self.model_type, implementation, build_type, probe_type)
            model = self.models[model_name]
            
            df = df.copy()
            df = df.drop(columns=['OPERATOR_IMPLEMENTATION', 'PREDICATE_COUNT', 'PRIMARY_PREDICATE'])            
            df = util.preprocess_data(df)
            df = df.rename(columns={
                'PROBE_TABLE_ROW_COUNT': 'PROBE_INPUT_ROWS',
                'BUILD_TABLE_ROW_COUNT': 'BUILD_INPUT_ROWS',
                'OUTPUT_ROW_COUNT': 'OUTPUT_ROWS',
                'BUILD_SORTED_0': 'BUILD_INPUT_COLUMN_SORTED_No',
                'BUILD_SORTED_1': 'BUILD_INPUT_COLUMN_SORTED_Ascending',
                'PROBE_SORTED_0': 'PROBE_INPUT_COLUMN_SORTED_No',
                'PROBE_SORTED_1': 'PROBE_INPUT_COLUMN_SORTED_Ascending'
            })

            df = df.drop(columns=['BUILDING_NS', 'BUILD_COLUMN', 'BUILD_SIDE', 'BUILD_SIDE_MATERIALIZING_NS', 'BUILD_TABLE', 'CLUSTERING_NS', 'DESCRIPTION', 'IS_FLIPPED', 'LEFT_COLUMN_NAME', 'LEFT_INPUT_OPERATOR_HASH', 'LEFT_TABLE_CHUNK_COUNT', 'LEFT_TABLE_NAME', 'LEFT_TABLE_ROW_COUNT', 'OPERATOR_HASH', 'OPERATOR_TYPE', 'OUTPUT_CHUNK_COUNT', 'OUTPUT_WRITING_NS', 'PROBE_COLUMN', 'PROBE_SIDE', 'PROBE_SIDE_MATERIALIZING_NS', 'PROBE_TABLE', 'PROBING_NS', 'QUERY_HASH', 'RADIX_BITS', 'RIGHT_COLUMN_NAME', 'RIGHT_COLUMN_TYPE_REFERENCE', 'RIGHT_INPUT_OPERATOR_HASH', 'RIGHT_TABLE_CHUNK_COUNT', 'RIGHT_TABLE_NAME', 'RIGHT_TABLE_ROW_COUNT'])
            optional_drop_columns = ['LEFT_COLUMN_TYPE_DATA', 'LEFT_COLUMN_TYPE_REFERENCE', 'RUNTIME_NS', 'BUILD_INPUT_COLUMN_SORTED_Ascending']
            df = df.drop(columns=optional_drop_columns, errors='ignore')
            df = util.append_to_input_format(df, self.model_formats[model_name])
            
            predictions = model.predict(df)
            self.join_estimates.loc[df.index, 'RUNTIME_ESTIMATE'] = np.array(predictions, dtype=np.int64)

            runtime += predictions.sum()
        print()
                        
        return runtime
    