from datetime import datetime
from .what_if_model import WhatIfModel

def extract_single_table(table_scans, table_name):
    return table_scans[table_scans['TABLE_NAME'] == table_name]

def get_correlations(benchmark_name):
    benchmark_name = benchmark_name.lower()
    if benchmark_name == "tpch":
        correlations = {
            'lineitem': {
                'l_shipdate': ['l_receiptdate', 'l_commitdate'],
                'l_receiptdate': ['l_shipdate', 'l_commitdate'],
            }
        }
    elif benchmark_name == "tpcds":
        correlations = dict()
    else:
        raise Exception("unknown benchmark, please provide correlation information")
        
    return correlations

  
def create_benchmark_configs(table_name=None, max_dimension=2, firstk=20):
    counter = 0
    
    start_time = datetime.now()
    clusterings = {}#{"default" : default_benchmark_config()}
    query_frequencies = get_query_frequencies()
    
    distinct_values = get_distinct_values_count()
    joins = load_join_statistics()    
    sorted_columns_during_creation = get_sorted_columns_during_creation()
    correlations = get_correlations()
    if table_name is not None:
        table_names = [table_name]
    else:
        table_names = get_table_names(scans, joins)
    
    print(table_names)
    for table_name in table_names:        
        start_time_table = datetime.now()
        single_table_scans = extract_single_table(scans, table_name)
        table_size = table_sizes[table_name]
        if table_size <= 3 * CHUNK_SIZE:
            print(f"Not computing clustering for {table_name}, as it has only {table_size} rows")
            continue        
        
        model = DisjointClustersModel(max_dimension, query_frequencies, table_name, single_table_scans, table_sizes, distinct_values, CHUNK_SIZE, correlations.get(table_name, {}), joins, sorted_columns_during_creation)
        table_clusterings = model.suggest_clustering(firstk)
        print(json.dumps(table_clusterings, indent=2))
        for table_clustering in table_clusterings:
            counter += 1
            #config = default_benchmark_config()
            config = {}
            config[table_name] = format_table_clustering(table_clustering)
            config_name = '{:02d}-'.format(counter) +  get_config_name(config)
            clusterings[config_name] = config
        end_time_table = datetime.now()
        print(f"Done computing clustering for {table_name} ({end_time_table - start_time_table})")
    
    end_time = datetime.now()
    print(f"Computed all clusterings in {end_time - start_time}")
    
    return clusterings

def format_table_clustering(clustering_config):
    # input format: List of [ [(column, split)+ ], sorting_column, runtime ]
    # output format: List of [ (column, split)+ ] - sorting column integrated if necessary
    
    assert len(clustering_config) == 3, "config should have exactly three entries: clustering columns, sort column, runtime"
    clustering_columns = clustering_config[0]
    assert len(clustering_columns) <= 3, "atm the model is at most 3-dimensional"
    #print(f"clustering columns are {clustering_columns}")
    last_clustering_column = clustering_columns[-1]
    last_clustering_column_name = last_clustering_column[0]
    #print(f"last column is {last_clustering_column_name}")
    sorting_column = clustering_config[1]
    #print(f"sort column is {sorting_column}")
    
    result = clustering_columns
    if last_clustering_column_name != sorting_column:
        result = clustering_columns + [(sorting_column, 1)]
        
    #print(f"in: {clustering_config}")
    #print(f"out: {result}")
    
    return result

def get_config_name(clustering_config):
    # Input: config-dict
    
    # List of lists. Each secondary list contains clustering information for a table
    table_configs = [clustering_config[table] for table in clustering_config]
    config_entries = [[f"{config_entry[0]}-{config_entry[1]}" for config_entry in config] for config in table_configs]
    table_entries = ["_".join(config) for config in config_entries]
    return "_".join(table_entries)



def create_model(table_name, input_parser, max_dimensions=2):
    input_parser.load_statistics()
    query_frequencies = input_parser.get_query_frequencies()
    distinct_values = input_parser.get_distinct_values_count()
    joins = input_parser.get_joins()
    sorted_columns_during_creation = input_parser.get_sorted_columns_during_creation()
    correlations = get_correlations(input_parser.benchmark_name)
    table_names = input_parser.get_table_names()
    start_time_table = datetime.now()
    single_table_scans = extract_single_table(input_parser.get_scans(), table_name)
    table_sizes = input_parser.get_table_sizes()
    CHUNK_SIZE = 65535

    model = WhatIfModel(max_dimensions, query_frequencies, table_name, single_table_scans, table_sizes, distinct_values, CHUNK_SIZE, correlations.get(table_name, {}), joins, sorted_columns_during_creation)
    return model