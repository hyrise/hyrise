#!/usr/bin/env python
result_table_path = '../../resources/test_data/tbl/join_operators/generated_tables/'

from join_test_configuration import DataType, PredicateCondition, JoinMode, ReferenceSegment, JoinTestConfiguration

# All possible shapes of each dimension

all_left_table_sizes = [0, 10, 15]
all_right_table_sizes = [0, 10, 15]
all_left_nulls = [True, False]
all_right_nulls = [True, False]
all_chunk_sizes = [0, 3, 10]
all_mpj = [1, 2]
all_swap_tables = [True, False]

# Define all test cases

from typing import List

result_configurations: List[JoinTestConfiguration] = []

for left_data_type in DataType:
    for right_data_type in DataType:
        # XOR
        if ((left_data_type == DataType.String) != (right_data_type == DataType.String)):
            continue

        join_test_configuration = JoinTestConfiguration.get_random()
        join_test_configuration.left_data_type = left_data_type
        join_test_configuration.right_data_type = right_data_type
        
        result_configurations.append(join_test_configuration)
        
for predicate_condition in PredicateCondition:
    for left_table_size in all_left_table_sizes:
        for right_table_size in all_right_table_sizes:
            join_test_configuration = JoinTestConfiguration.get_random()
            join_test_configuration.predicate_condition = predicate_condition
            join_test_configuration.left_table_size = left_table_size
            join_test_configuration.right_table_size = right_table_size
                
            result_configurations.append(join_test_configuration)
                
for left_table_size in all_left_table_sizes:
    for right_table_size in all_right_table_sizes:
        for chunk_size in all_chunk_sizes:
            join_test_configuration = JoinTestConfiguration.get_random()
            join_test_configuration.left_table_size = left_table_size
            join_test_configuration.right_table_size = right_table_size
            join_test_configuration.chunk_size = chunk_size
                
            result_configurations.append(join_test_configuration)

for join_mode in JoinMode:
    for left_null in all_left_nulls:
        for right_null in all_right_nulls:
            join_test_configuration = JoinTestConfiguration.get_random()
            join_test_configuration.join_mode = join_mode
            join_test_configuration.left_null = left_null
            join_test_configuration.right_null = right_null
                
            result_configurations.append(join_test_configuration)

for join_mode in JoinMode:
    for left_table_size in all_left_table_sizes:
        for right_table_size in all_right_table_sizes:
            join_test_configuration = JoinTestConfiguration.get_random()
            join_test_configuration.join_mode = join_mode
            join_test_configuration.left_table_size = left_table_size
            join_test_configuration.right_table_size = right_table_size
                
            result_configurations.append(join_test_configuration)

for predicate_condition in PredicateCondition:
    for join_mode in JoinMode:
        for swap_table in all_swap_tables:
            join_test_configuration = JoinTestConfiguration.get_random()
            join_test_configuration.join_mode = join_mode
            join_test_configuration.predicate_condition = predicate_condition
            join_test_configuration.swap_table = swap_table
                
            result_configurations.append(join_test_configuration)

for left_reference_segment in ReferenceSegment:
    for right_reference_segment in ReferenceSegment:
        join_test_configuration = JoinTestConfiguration.get_random()
        join_test_configuration.left_reference_segment = left_reference_segment
        join_test_configuration.right_reference_segment = right_reference_segment
                
        result_configurations.append(join_test_configuration)

for join_mode in JoinMode:
    for mpj in all_mpj:   
        join_test_configuration = JoinTestConfiguration.get_random()
        join_test_configuration.join_mode = join_mode
        join_test_configuration.mpj = mpj
        
        result_configurations.append(join_test_configuration)


import json

json_configs = [json.loads(conf.to_json()) for conf in result_configurations]
with open(result_table_path + 'join_configurations.json', 'w') as outfile:  
    json.dump(json_configs, outfile, indent=4)

    

