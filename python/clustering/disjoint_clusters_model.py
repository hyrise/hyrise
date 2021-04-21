import math
import numpy as np
import operator
import pandas as pd
from functools import reduce

from .single_table_model import SingleTableMdcModel

class DisjointClustersModel(SingleTableMdcModel):
    
    def __init__(self, max_dimensions, query_frequencies, table_name, table_scans, table_sizes, distinct_values, target_chunksize, correlations, joins, aggregates, sorted_columns_during_creation):
        super().__init__(max_dimensions, query_frequencies, table_name, table_scans, table_sizes, distinct_values, target_chunksize, correlations, joins, aggregates, sorted_columns_during_creation)
        
    # This function should only be called for the own table, not for others
    def estimate_distinct_values_per_chunk(self, column, clustering_columns, chunk_sorting_column, dimension_cardinalities):
        total_distinct_values = self.distinct_values[self.table_name][column]
        
        if column in clustering_columns:
            index = clustering_columns.index(column)
            clusters_for_column = dimension_cardinalities[index]
            return min(self.target_chunksize, total_distinct_values / clusters_for_column)
        else:
            # TODO cluster wise sorting
            return min(self.target_chunksize, total_distinct_values)
        
    # This function should only be called for the own table, not for others
    def estimate_chunk_count(self, query_hash, join_input_rows, clustering_columns, dimension_cardinalities):        
        # TODO include or exclude scans that do not benefit from pruning? Excluded for now        
        table_scans = self.table_scans[self.table_scans['QUERY_HASH'] == query_hash]        
        table_scans = table_scans[table_scans['useful_for_pruning']]
        
        if len(table_scans) > 0:
            #print(f"For query hash {query_hash}, there are {len(table_scans)} scans on {self.table_name}")
            def get_denseness_factor(row):
                column = row['COLUMN_NAME']
                if column in clustering_columns:
                    # TODO more precise estimate
                    denseness_factor = 1
                else:
                    denseness_factor = row['selectivity']

                return denseness_factor

            denseness_factors = table_scans.apply(get_denseness_factor, axis=1)
            denseness_factor = denseness_factors.product()
            #print(f"denseness factor is {denseness_factor}")
        else:
            denseness_factor = 1        
        
        chunk_count = math.ceil(join_input_rows / (self.target_chunksize * denseness_factor))
        max_chunks = math.ceil(self.table_size / self.target_chunksize)
        #if chunk_count > max_chunks:
        #    print(f"WARNING: estimated {chunk_count} chunks, but {self.table_name} got only {max_chunks}\nDenseness: {denseness_factor}")
        
        
        return min(chunk_count, max_chunks)
    
    def statistic_time_dimension_cardinalities(self):
        return math.ceil(self.table_size / self.target_chunksize)
    
    def get_dimension_cardinalities(self, clustering_columns):
        MAX_HISTOGRAM_BINS = 100
        target_cluster_count = math.ceil(self.table_size / self.target_chunksize)
    
        join_columns = list(filter(lambda x: self.is_join_column(x), clustering_columns))
        scan_columns = list(filter(lambda x: self.is_scan_column(x), clustering_columns))
        intersecting_columns = set(join_columns).intersection(set(scan_columns))
        assert len(intersecting_columns) == 0, f"The following columns are used as both join and scan column: {intersecting_columns}"
        
        #scan_columns = clustering_columns.copy()
        #join_columns = []
        
        if len(scan_columns) == 0:
            CLUSTERS_PER_JOIN_COLUMN = math.ceil(math.pow(target_cluster_count, 1/len(join_columns)))
        else: 
            CLUSTERS_PER_JOIN_COLUMN = 3;
        # Assumption: uniform distribution (in the sense that every cluster actually exists)
        num_join_clusters = math.pow(CLUSTERS_PER_JOIN_COLUMN, len(join_columns))
        assert num_join_clusters <= 2 * target_cluster_count, f"Would get {num_join_clusters} clusters for join columns, but aimed at at most {target_cluster_count} clusters"
    
        # only applies to scan columns
        desired_scan_clusters_count = math.ceil(target_cluster_count / num_join_clusters)
        individual_distinct_values = [self.distinct_values[self.table_name][column] for column in scan_columns]
        log_distinct_values = [math.ceil(0.5+np.log2(x)) for x in individual_distinct_values]
        log_distinct_values_product = reduce(operator.mul, log_distinct_values, 1)
        assert log_distinct_values_product > 0, "cannot have a distinct value count of 0"

        global_modification_factor = desired_scan_clusters_count / log_distinct_values_product
        num_scan_dimensions = len(scan_columns)
        individual_modification_factor = np.power(global_modification_factor, 1.0 / max(1, num_scan_dimensions))
        
        join_column_cluster_counts = [CLUSTERS_PER_JOIN_COLUMN] * len(join_columns)
        scan_column_cluster_counts = [math.ceil(x * individual_modification_factor) for x in log_distinct_values]
        
        
        # Merge join and scan columns
        join_index = 0
        scan_index = 0
        cluster_counts = []
        for clustering_column in clustering_columns:
            if clustering_column in join_columns:
                cluster_counts.append(join_column_cluster_counts[join_index])
                join_index += 1
            elif clustering_column in scan_columns:
                cluster_counts.append(scan_column_cluster_counts[scan_index])
                scan_index += 1
        assert join_index == len(join_columns), f"Processed the wrong number of join columns: {join_index} instead of {len(join_column_cluster_counts)}"
        assert scan_index == len(scan_columns), f"Processed the wrong number of scan columns: {scan_index} instead of {len(scan_column_cluster_counts)}"
        assert len(cluster_counts) == len(clustering_columns), f"Expected {len(clustering_columns)} cluster counts, but got {len(cluster_counts)}"
        
        # incorporate that we cannot have more than MAX_HISTOGRAM_BINS clusters per column
        max_cluster_counts = []
        maximum_cluster_count_reached = []
        for index, clustering_column in enumerate(clustering_columns):
            max_clusters = min(MAX_HISTOGRAM_BINS, self.distinct_values[self.table_name][clustering_column])
            max_cluster_counts.append(max_clusters)
            if cluster_counts[index] > max_clusters:
                #print(f"Reducing cluster count for {clustering_column} from {cluster_counts[index]} to {max_clusters}")
                cluster_counts[index] = max_clusters
            maximum_cluster_count_reached.append(cluster_counts[index] >= max_clusters)
        
        #total_cluster_count = np.prod(cluster_counts)
        #growable_dimension_count = len(maximum_cluster_count_reached) - sum(maximum_cluster_count_reached)
        #if total_cluster_count < target_cluster_count and growable_dimension_count > 1:
        #    grow_factor = (target_cluster_count / total_cluster_count) ** (1/growable_dimension_count)
        #    for index, clustering_column in enumerate(clustering_columns):
        #        current = cluster_counts[index]
        #        new = min(max_cluster_counts[index], math.floor(grow_factor * current))                
        #        cluster_counts[index] = new
                #if current < new:
                #    print(f"Increasing the cluster count of {clustering_column} from {current} to {new}")
                #else:
                #    print(f"Cannot increase the cluster count of {clustering_column}, because it already is at {current}")
                
        
        # testing
        actual_cluster_count = reduce(operator.mul, cluster_counts, 1)
        assert actual_cluster_count > 0, "there was a split up factor of 0"
        assert actual_cluster_count <= 2 * target_cluster_count, f"Wanted at most {target_cluster_count} clusters, but got {actual_cluster_count}\nConfig: {clustering_columns}\nCluster sizes: {cluster_counts}"
        estimated_chunksize = self.table_size / actual_cluster_count

        #assert estimated_chunksize <= self.target_chunksize, f"chunks should be smaller, not larger than target_chunksize. Estimated chunk size is {estimated_chunksize}"
        allowed_percentage = 0.55
        if estimated_chunksize < allowed_percentage * self.target_chunksize:
            print(f"Warning: chunks should not be too much smaller than target_chunksize: {estimated_chunksize} < {allowed_percentage} * {self.target_chunksize}")
        #assert estimated_chunksize >= allowed_percentage * self.target_chunksize, f"chunks should not be too much smaller than target_chunksize: {estimated_chunksize} < {allowed_percentage} * {self.target_chunksize}"
        
        return cluster_counts
        
    
    
    def get_dimension_cardinalities2(self, clustering_columns):
        # ToDo what if we aim at less than number of chunks clusters, i.e. multiple chunks per cluster?
        target_cluster_count = math.ceil(1.1 * self.table_size / self.target_chunksize)
        # idea: fixed size for join columns, variable amount for scan columns
        
        join_columns = list(filter(lambda x: self.is_join_column(x), clustering_columns))
        scan_columns = list(filter(lambda x: self.is_scan_column(x), clustering_columns))
        intersecting_columns = set(join_columns).intersection(set(scan_columns))
        assert len(intersecting_columns) == 0, f"The following columns are used as both join and scan column: {intersecting_columns}"
        
        if len(scan_columns) == 0:
            CLUSTERS_PER_JOIN_COLUMN = math.ceil(math.pow(target_cluster_count, 1/len(join_columns)))
        else: 
            CLUSTERS_PER_JOIN_COLUMN = 3;
        # Assumption: uniform distribution (in the sense that every cluster actually exists)
        num_join_clusters = math.pow(CLUSTERS_PER_JOIN_COLUMN, len(join_columns))
        assert num_join_clusters <= 2 * target_cluster_count, f"Would get {num_join_clusters} clusters for join columns, but aimed at at most {target_cluster_count} clusters"
    
        # only applies to scan columns
        desired_scan_clusters_count = math.ceil(target_cluster_count / num_join_clusters)
        individual_distinct_values = [self.distinct_values[self.table_name][column] for column in scan_columns]
        log_distinct_values = [math.ceil(0.5+np.log2(x)) for x in individual_distinct_values]
        log_distinct_values_product = reduce(operator.mul, log_distinct_values, 1)
        assert log_distinct_values_product > 0, "cannot have a distinct value count of 0"

        global_modification_factor = desired_scan_clusters_count / log_distinct_values_product
        num_scan_dimensions = len(scan_columns)
        individual_modification_factor = np.power(global_modification_factor, 1.0 / max(1, num_scan_dimensions))
        
        join_column_cluster_counts = [CLUSTERS_PER_JOIN_COLUMN] * len(join_columns)
        scan_column_cluster_counts = [math.ceil(x * individual_modification_factor) for x in log_distinct_values]
        
        
        # Merge join and scan columns
        join_index = 0
        scan_index = 0
        cluster_counts = []
        for clustering_column in clustering_columns:
            if clustering_column in join_columns:
                cluster_counts.append(join_column_cluster_counts[join_index])
                join_index += 1
            elif clustering_column in scan_columns:
                cluster_counts.append(scan_column_cluster_counts[scan_index])
                scan_index += 1
        assert join_index == len(join_columns), f"Processed the wrong number of join columns: {join_index} instead of {len(join_column_cluster_counts)}"
        assert scan_index == len(scan_columns), f"Processed the wrong number of scan columns: {scan_index} instead of {len(scan_column_cluster_counts)}"
        assert len(cluster_counts) == len(clustering_columns), f"Expected {len(clustering_columns)} cluster counts, but got {len(cluster_counts)}"
        
        # testing
        actual_cluster_count = reduce(operator.mul, cluster_counts, 1)
        assert actual_cluster_count > 0, "there was a split up factor of 0"
        assert actual_cluster_count <= 2 * target_cluster_count, f"Wanted at most {target_cluster_count} clusters, but got {actual_cluster_count}\nConfig: {clustering_columns}\nCluster sizes: {cluster_counts}"
        estimated_chunksize = self.table_size / actual_cluster_count

        assert estimated_chunksize <= self.target_chunksize, f"chunks should be smaller, not larger than target_chunksize. Estimated chunk size is {estimated_chunksize}"
        allowed_percentage = 0.55
        if estimated_chunksize < allowed_percentage * self.target_chunksize:
            print(f"Warning: chunks should not be too much smaller than target_chunksize: {estimated_chunksize} < {allowed_percentage} * {self.target_chunksize}")
        #assert estimated_chunksize >= allowed_percentage * self.target_chunksize, f"chunks should not be too much smaller than target_chunksize: {estimated_chunksize} < {allowed_percentage} * {self.target_chunksize}"
        
        return cluster_counts