import pandas as pd
import numpy as np
import math
import itertools
from functools import reduce

from .abstract_model import AbstractModel

class SingleTableMdcModel(AbstractModel):
    
    def __init__(self, max_dimensions, query_frequencies, table_name, table_scans, table_sizes, distinct_values, target_chunksize, correlations, joins, aggregates, sorted_columns_during_creation):
        super().__init__(query_frequencies, table_name, table_scans, correlations)
        self.max_dimensions = max_dimensions
        self.table_sizes = table_sizes       
        self.table_size = table_sizes[table_name]
        self.distinct_values = distinct_values
        self.target_chunksize = target_chunksize
        self.joins = joins
        self.aggregates = aggregates
        self.sorted_columns_during_creation = sorted_columns_during_creation
        
        
        self.join_column_names = self.extract_join_columns()
        self.scan_column_names = self.extract_scan_columns()
        
        self.scan_estimates = pd.DataFrame()
        self.scan_estimates['QUERY_HASH'] = self.table_scans['QUERY_HASH']        
        self.scan_estimates['DESCRIPTION'] = self.table_scans['DESCRIPTION']
        self.scan_estimates['RUNTIME_ESTIMATE'] = np.array([-1] * len(self.table_scans))
        self.scan_estimates['RUNTIME_NS'] = self.table_scans['RUNTIME_NS'] # TODO: probably not necessary?
        self.scan_estimates['time_per_input_row'] = self.table_scans['time_per_input_row']
        #self.scan_estimates.index = self.table_scans.index
        
        self.join_estimates = pd.DataFrame()
        self.join_estimates['QUERY_HASH'] = self.joins['QUERY_HASH']
        self.join_estimates['DESCRIPTION'] = self.joins['DESCRIPTION']
        self.join_estimates['ESTIMATE_BUILD_SIDE_MATERIALIZING'] = np.array([-1] * len(self.joins))
        self.join_estimates['ESTIMATE_PROBE_SIDE_MATERIALIZING'] = np.array([-1] * len(self.joins))
        self.join_estimates['ESTIMATE_CLUSTERING'] = np.array([-1] * len(self.joins))
        self.join_estimates['ESTIMATE_BUILDING'] = np.array([-1] * len(self.joins))
        self.join_estimates['ESTIMATE_PROBING'] = np.array([-1] * len(self.joins))
        self.join_estimates['ESTIMATE_OUTPUT_WRITING'] = np.array([-1] * len(self.joins))
        self.join_estimates['BUILD_SIDE_MATERIALIZING_NS'] = self.joins['BUILD_SIDE_MATERIALIZING_NS']
        self.join_estimates['PROBE_SIDE_MATERIALIZING_NS'] = self.joins['PROBE_SIDE_MATERIALIZING_NS']
        self.join_estimates['CLUSTERING_NS'] = self.joins['CLUSTERING_NS']
        self.join_estimates['BUILDING_NS'] = self.joins['BUILDING_NS']
        self.join_estimates['PROBING_NS'] = self.joins['PROBING_NS']
        self.join_estimates['OUTPUT_WRITING_NS'] = self.joins['OUTPUT_WRITING_NS']
        self.join_estimates['RUNTIME_NS'] = self.joins['RUNTIME_NS']

        #self.aggregate_estimates = pd.DataFrame()
        #self.aggregate_estimates['QUERY_HASH'] = self.aggregates['QUERY_HASH']
        #self.aggregate_estimates['DESCRIPTION'] = self.aggregates['DESCRIPTION']
        #self.aggregate_estimates['RUNTIME_NS'] = self.aggregates['RUNTIME_NS']
        #self.aggregate_estimates['RUNTIME_ESTIMATE'] = np.array([-1] * len(self.aggregates))
        
    def is_join_column(self, column_name):
        return column_name in self.join_column_names
    
    def is_scan_column(self, column_name):
        return column_name in self.scan_column_names
    
    def suggest_clustering(self, first_k=1):
        interesting_columns = self.extract_interesting_columns()

        print(interesting_columns)
        clustering_columns = itertools.combinations_with_replacement(interesting_columns, self.max_dimensions)
        clustering_columns = [self.uniquify(clustering) for clustering in clustering_columns]
        print(f"There are {len(clustering_columns)} clustering column sets")
        
        reduced_clustering_columns = []
        seen_clusterings = set()
        for clustering in clustering_columns:
            clustering_string = "-".join(clustering)
            if clustering_string not in seen_clusterings:
                seen_clusterings.add(clustering_string)
                reduced_clustering_columns.append(clustering)
        clustering_columns = reduced_clustering_columns
        
        
        print(f"{len(clustering_columns)} of them are unique")
        sort_columns = interesting_columns        
        clusterings_with_runtimes = reduce(lambda x,y: x+y,[self.estimate_total_runtimes(clustering_cols, sort_columns) for clustering_cols in clustering_columns])
        clusterings_with_runtimes.sort(key=lambda x: x[2], reverse=False)
        
        return clusterings_with_runtimes[0:first_k]
    
    
    def estimate_distinct_values_per_chunk(self, column, clustering_columns, sorting_column, dimension_cardinalities):
        raise NotImplementedError("Each model should provide this function")
        
    def estimate_distinct_values_per_chunk_at_statistics_time(self, column, table):        
        if column in self.sorted_columns_during_creation.get(table, {}):
            # Column was globally sorted
            average_count_per_distinct_value = self.table_sizes[table] / self.distinct_values[table][column]
            return math.ceil(self.target_chunksize / average_count_per_distinct_value)
        else:
            # Column was not globally sorted
            total_distinct_values = self.distinct_values[table][column]
            return min(total_distinct_values, self.target_chunksize)        
    
    def compute_unprunable_parts(self, row, clustering_columns, split_factors):
        def clustering_columns_correlated_to(column):
            return [clustering_column for clustering_column in clustering_columns if column in self.correlations.get(clustering_column, {})]

        def correlates_to_clustering_column(column):
            return len(clustering_columns_correlated_to(column)) > 0

        column_name = row['COLUMN_NAME']

        if not row['useful_for_pruning']:
            selectivity = 1
        elif column_name in clustering_columns:
            scan_selectivity = row['SELECTIVITY_LEFT'] # TODO: originally: selectivity
            split_factor = split_factors[clustering_columns.index(column_name)]
            selectivity =  self.round_up_to_next_multiple(scan_selectivity, 1 / split_factor)
        elif correlates_to_clustering_column(column_name):
            scan_selectivity = row['SELECTIVITY_LEFT'] # TODO: originally: selectivity
            correlated_clustering_columns = clustering_columns_correlated_to(column_name)

            # ToDo this is hacky, but for now assume there is just one correlated column
            assert len(correlated_clustering_columns) == 1, f"expected just 1 correlated clustering column, but got {len(correlated_clustering_columns)}"

            split_factor = split_factors[clustering_columns.index(correlated_clustering_columns[0])]
            selectivity = min(1, 1.2 * self.round_up_to_next_multiple(scan_selectivity, 1 / split_factor))
        else:
            selectivity = 1

        return selectivity
    
    def estimate_table_scan_runtime(self, clustering_columns, sorting_column, split_factors):                
        def compute_tablescan_runtime(row, sorting_column):
            assert row['estimated_input_rows'] > 1, row
            assert row['runtime_per_input_row'] > 0, row
            assert row['runtime_per_output_row'] > 0, row
            input_row_count = row['estimated_input_rows']
            
            if row['COLUMN_NAME'] == sorting_column and row['benefits_from_sorting'] and not row['COLUMN_NAME'] in self.sorted_columns_during_creation.get(self.table_name, {}):
                # TODO is this the best way to simulate sorted access?
                input_row_count = np.log2(input_row_count)

            runtime = input_row_count * row['runtime_per_input_row'] + row['OUTPUT_ROW_COUNT'] * row['runtime_per_output_row']
            return runtime * self.query_frequency(row['QUERY_HASH'])
        
        original_clustering_column = self.sorted_columns_during_creation[self.table_name][0]
        
        
        runtime = 0
        scans_per_query = self.table_scans.sort_values(['INPUT_ROW_COUNT'], ascending=False).groupby(['QUERY_HASH', 'GET_TABLE_HASH'])
        for _, scans in scans_per_query:
            number_of_scans = len(scans)
            assert number_of_scans > 0 and number_of_scans < 25, f"weird scan length: {number_of_scans}\nScans:\n{scans}"
            # TODO: kinda unrealistic assumption: everything not in the table scan result can be pruned            

            unprunable_parts = scans.apply(self.compute_unprunable_parts, axis=1, args=(clustering_columns, split_factors,))
            unprunable_part = unprunable_parts.product()
            assert unprunable_part > 0, "no unprunable part"
            
            estimated_pruned_table_size = min(self.table_size, self.round_up_to_next_multiple(unprunable_part * self.table_size, CHUNK_SIZE))
            
            runtimes = pd.DataFrame()
            runtimes['QUERY_HASH'] = scans['QUERY_HASH']
            runtimes['runtime_per_input_row'] = scans['time_per_input_row']
            runtimes['runtime_per_output_row'] = scans['time_per_output_row']
            runtimes['COLUMN_NAME'] = scans['COLUMN_NAME']
            runtimes['benefits_from_sorting'] = scans['benefits_from_sorting']
            # the pruned table inputs should be reflected in 'estimated_input_rows'
            runtimes['estimated_input_rows'] = scans['INPUT_ROW_COUNT']
            runtimes['OUTPUT_ROW_COUNT'] = scans['OUTPUT_ROW_COUNT']

            runtimes.iloc[0, runtimes.columns.get_loc('estimated_input_rows')] = estimated_pruned_table_size
            assert runtimes['estimated_input_rows'].iloc[0] == estimated_pruned_table_size, f"value is {runtimes.iloc[0]['estimated_input_rows']}, but should be {estimated_pruned_table_size}"
            # TODO modify input sizes of subsequent scans
            
            scan_runtimes = runtimes.apply(compute_tablescan_runtime, axis=1, args=(sorting_column,))
            #self.table_scans.iloc[:, self.table_scans.columns.get_loc('RUNTIME_ESTIMATE')] = scan_runtimes
            self.scan_estimates.loc[scan_runtimes.index, 'RUNTIME_ESTIMATE'] = scan_runtimes
            runtime += scan_runtimes.sum()
        return runtime

    def estimate_aggregate_runtime(effective_clustering_columns, sorting_column, effective_dimension_cardinalities):
        raise NotImplementedError("Subclass responsibility")

    def estimate_chunk_count(self, scans, clustering_columns, dimension_cardinalities):
        raise NotImplementedError("Subclass responsibility")
    
    def get_chunk_count_factor(self, row, side, clustering_columns, dimension_cardinalities):
        query_hash = row['QUERY_HASH']
        if side == "PROBE":
            input_rows = row['PROBE_TABLE_ROW_COUNT']
            assert row['PROBE_TABLE'] == self.table_name, "Call this function only for the own table"
            
            # When joining tables, the table size might increase (a lot). This makes it hard to estimate the chunk count, so just ignore it
            if input_rows > self.table_size:
                return 1
        elif side == "BUILD":
            input_rows = row['BUILD_TABLE_ROW_COUNT']
            assert row['BUILD_TABLE'] == self.table_name, "Call this function only for the own table"
            # When joining tables, the table size might increase (a lot). This makes it hard to estimate the chunk count, so just ignore it
            if input_rows > self.table_size:
                return 1
        else:
            raise ValueError("side must be PROBE or BUILD")

        expected_chunk_count = self.estimate_chunk_count(query_hash, input_rows, clustering_columns, dimension_cardinalities)
        
        min_chunk_count = math.ceil(input_rows / self.target_chunksize)
        max_chunk_count = math.ceil(self.table_size / self.target_chunksize)        

        CHUNK_COUNT_SPEEDUP_LOW = 3
        CHUNK_COUNT_SPEEDUP_HIGH = 1

        current_speedup = self.interpolate(CHUNK_COUNT_SPEEDUP_LOW, CHUNK_COUNT_SPEEDUP_HIGH, (expected_chunk_count - min_chunk_count) / (max_chunk_count + 0.01 - min_chunk_count))
        #print(f"current speedup: {current_speedup}")

        old_clustering_columns = self.sorted_columns_during_creation[self.table_name]
        old_dimension_cardinalities = [self.statistic_time_dimension_cardinalities()] * len(old_clustering_columns)
        old_expected_chunk_count = self.estimate_chunk_count(query_hash, input_rows, old_clustering_columns, old_dimension_cardinalities)
        old_speedup = self.interpolate(CHUNK_COUNT_SPEEDUP_LOW, CHUNK_COUNT_SPEEDUP_HIGH, (old_expected_chunk_count - min_chunk_count) / (max_chunk_count + 0.01 - min_chunk_count))
        #print(f"old speedup: {old_speedup}")

        return 1 * old_speedup / current_speedup
    
    def estimate_join_runtime(self, clustering_columns, sorting_column, dimension_cardinalities):
        def compute_join_runtime(row, sorting_column):
            if "JoinHash" in row['DESCRIPTION']:
                probe_column = row['PROBE_COLUMN']
                if row['PROBE_TABLE'] == self.table_name:
                    probe_column_was_sorted = row['PROBE_SORTED'] and probe_column in self.sorted_columns_during_creation.get(self.table_name, {})
                    probe_column_is_sorted = row['PROBE_SORTED'] and probe_column == sorting_column
                    materialize_probe_factor = self.get_chunk_count_factor(row, "PROBE", clustering_columns, dimension_cardinalities)
                else:
                    probe_column_was_sorted = row['PROBE_SORTED'] and probe_column in self.sorted_columns_during_creation.get(row['PROBE_TABLE'], {})
                    probe_column_is_sorted = probe_column_was_sorted
                    materialize_probe_factor = 1
                    
                build_column = row['BUILD_COLUMN']
                if row['BUILD_TABLE'] == self.table_name:
                    build_column_was_sorted = row['BUILD_SORTED'] and build_column in self.sorted_columns_during_creation.get(self.table_name, {})
                    build_column_is_sorted = row['BUILD_SORTED'] and build_column == sorting_column
                    materialize_build_factor = self.get_chunk_count_factor(row, "BUILD", clustering_columns, dimension_cardinalities)
                else:
                    build_column_was_sorted = row['BUILD_SORTED'] and build_column in self.sorted_columns_during_creation.get(row['BUILD_TABLE'], {})
                    build_column_is_sorted = build_column_was_sorted
                    materialize_build_factor = 1

                time_materialize_probe = row['PROBE_SIDE_MATERIALIZING_NS']
                time_materialize_build = row['BUILD_SIDE_MATERIALIZING_NS']                
                
                def get_materialize_factor(column, was_globally_sorted, is_sorted, expected_distinct_value_count):
                    # Assumption: "was_sorted" implies global sortedness, i.e., both clustering and chunkwise sorting
                    # This is true when the clustering produced by the table generator is used by the plan cache exporter
                    # If the data has been re-clustered before the plan cache exporter runs, there has to be some system inside Hyrise which tracks the current clustering config
                    
                    # Sortedness seems to yield a speed up of approx. 1.6, regardless of the number of distinct values
                    SORT_SPEEDUP = 1.6
                    sortedness_factor = 1                    
                    was_sorted = was_globally_sorted
                    if was_sorted:
                        sortedness_factor *= SORT_SPEEDUP
                    if is_sorted:
                        sortedness_factor /= SORT_SPEEDUP
                    

                    # The influence of clustering depends on the number of distinct values
                    CLUSTERING_SPEEDUP_LOW = 1.84
                    CLUSTERING_SPEEDUP_HIGH = 1
                    clustering_factor = 1
                    statistics_time_distinct_value_count = self.estimate_distinct_values_per_chunk_at_statistics_time(column, self.table_name);
                    clustering_factor *= self.interpolate(CLUSTERING_SPEEDUP_LOW, CLUSTERING_SPEEDUP_HIGH, statistics_time_distinct_value_count / self.target_chunksize)
                    clustering_factor /= self.interpolate(CLUSTERING_SPEEDUP_LOW, CLUSTERING_SPEEDUP_HIGH, expected_distinct_value_count / self.target_chunksize)
                        
                    return sortedness_factor * clustering_factor                
                
                if row['PROBE_TABLE'] == self.table_name and row['PROBE_SORTED']:
                    expected_distinct_values_probe = self.estimate_distinct_values_per_chunk(probe_column, clustering_columns, sorting_column, dimension_cardinalities)
                    materialize_probe_factor *= get_materialize_factor(probe_column, probe_column_was_sorted, probe_column_is_sorted, expected_distinct_values_probe)
                    
                if row['BUILD_TABLE'] == self.table_name and row['BUILD_SORTED']:
                    expected_distinct_values_build = self.estimate_distinct_values_per_chunk(build_column, clustering_columns, sorting_column, dimension_cardinalities)
                    materialize_build_factor *= get_materialize_factor(build_column, build_column_was_sorted, build_column_is_sorted, expected_distinct_values_build)
                
                time_materialize = time_materialize_probe * materialize_probe_factor + time_materialize_build *  materialize_build_factor

                #print(f"row.name is {row.name}")
                self.join_estimates.loc[row.name, 'ESTIMATE_BUILD_SIDE_MATERIALIZING'] =  time_materialize_build *  materialize_build_factor
                self.join_estimates.loc[row.name, 'ESTIMATE_PROBE_SIDE_MATERIALIZING'] =  time_materialize_probe *  materialize_probe_factor

                # unchanged
                time_cluster = row['CLUSTERING_NS']
                self.join_estimates.loc[row.name, 'ESTIMATE_CLUSTERING'] = time_cluster

                # unchanged
                time_build = row['BUILDING_NS']
                self.join_estimates.loc[row.name, 'ESTIMATE_BUILDING'] =  time_build


                time_probe = row['PROBING_NS']
                probe_factor = 1
                if probe_column_is_sorted:
                    if not probe_column_was_sorted:
                        probe_factor = 0.7
                    else:
                        probe_factor = 1
                else:
                    if probe_column_was_sorted:
                        probe_factor = 1.3
                    else:
                        probe_factor = 1
                
                #elif probe_column_is_sorted or probe_column_is_clustered:
                #    if not probe_column_was_sorted:
                #        probe_factor = 0.9
                #    else:
                #        probe_factor = 1.1
                #elif probe_column_was_sorted:
                #    # probe column is now neither sorted nor clustered
                #    probe_factor = 1.4

                time_probe *= probe_factor                
                self.join_estimates.loc[row.name, 'ESTIMATE_PROBING'] =  time_probe

                # unchanged
                time_write_output = row['OUTPUT_WRITING_NS']
                self.join_estimates.loc[row.name, 'ESTIMATE_OUTPUT_WRITING'] =  time_write_output



                # TODO: how to deal with the difference between RUNTIME_NS and sum(stage_runtimes)?
                runtime = time_materialize + time_cluster + time_build + time_probe + time_write_output
            else:
                runtime = row['RUNTIME_NS']

            return runtime * self.query_frequency(row['QUERY_HASH'])

        join_runtimes = self.joins.apply(compute_join_runtime, axis=1, args=(sorting_column,))
        return join_runtimes.sum()

    def effective_clustering(self, clustering_columns, sort_column, cardinalities):
        expected_num_clusters = np.prod(cardinalities)
        chunks_in_table = math.ceil(self.table_size / self.target_chunksize)
        expected_chunks_per_cluster = chunks_in_table / expected_num_clusters
        #if expected_chunks_per_cluster < 1:
        #    print(f"WARNING: expected chunks per cluster is below 1: {expected_chunks_per_cluster}")

        if round(expected_chunks_per_cluster) < 2:
            return clustering_columns, cardinalities
        
        columns = clustering_columns.copy()
        counts = cardinalities.copy()
        if sort_column not in columns:
            columns.append(sort_column)
            counts.append(1)
            
        counts[columns.index(sort_column)] = round(counts[columns.index(sort_column)] * expected_chunks_per_cluster)
        assert len(counts) == len(columns)
        
        #print(f"effective columns changed from {clustering_columns} to {columns}")
        #print(f"effective counts changed from {cardinalities} to {counts}")
        
        return columns, counts
            
    
    def estimate_total_runtimes(self, clustering_columns, sorting_columns):
        #print(f"testing clustering {clustering_columns} with sorting columns {sorting_columns}")
        dimension_cardinalities = self.get_dimension_cardinalities(clustering_columns)
        total_runtimes = {sorting_column: 0 for sorting_column in sorting_columns}
        clusterings = []
        for sorting_column in sorting_columns:
            runtime = self.estimate_total_runtime(clustering_columns, sorting_column, dimension_cardinalities)
            clusterings.append([list(zip(clustering_columns, dimension_cardinalities)), sorting_column, runtime])
        
        #clusterings = [[list(zip(clustering_columns, dimension_cardinalities)), sorting_column, np.int64(total_runtimes[sorting_column])] for sorting_column in sorting_columns]
        return clusterings
    
    def estimate_total_runtime(self, clustering_columns, sorting_column, dimension_cardinalities):
        effective_clustering_columns, effective_dimension_cardinalities = self.effective_clustering(clustering_columns, sorting_column, dimension_cardinalities)                    
        
        runtime = 0
        runtime += self.estimate_table_scan_runtime(effective_clustering_columns, sorting_column, effective_dimension_cardinalities)
        runtime += self.estimate_join_runtime(effective_clustering_columns, sorting_column, effective_dimension_cardinalities)
        runtime += self.estimate_aggregate_runtime(effective_clustering_columns, sorting_column, effective_dimension_cardinalities)
        
        return runtime
    
    def get_dimension_cardinalities(self, clustering_columns):
        raise NotImplementedError("Subclasses must override this function")
        
    def statistic_time_dimension_cardinalities(self):
        raise NotImplementedError("Subclasses must override this function")
        
    def interpolate(self, low, high, percentage):
        assert percentage >= 0 and percentage <= 1, f"percentage must between 0 and 1, but is {percentage}"
        return (1 - percentage) * low + (percentage * high)