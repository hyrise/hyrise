#!/usr/bin/env python
from enum import Enum

result_table_path = '../../resources/test_data/tbl/join_operators/generated_tables/'

class DataType(Enum):
    Int = 'int'
    Long = 'long'
    Float = 'float'
    Double = 'double'
    String = 'string'
    
class PredicateCondition(Enum):
    Equals = 'Equals'
    NotEquals = 'NotEquals'
    LessThan = 'LessThan'
    LessThanEquals = 'LessThanEquals'
    GreaterThan = 'GreaterThan'
    GreaterThanEquals = 'GreaterThanEquals'
    
class JoinMode(Enum):
    Inner = 'Inner'
    Left = 'Left'
    Right = 'Right'
    Full = 'Full'
    Semi = 'Semi'
    AntiNullAsTrue = 'AntiNullAsTrue'
    AntiNullAsFalse = 'AntiNullAsFalse'

class ReferenceSegment(Enum):
    Yes = 'Yes'
    No = 'No'
    Join = 'Join'

# All possible shapes of each dimension

all_left_table_sizes = [0, 10, 15]
all_right_table_sizes = [0, 10, 15]
all_left_nulls = [True, False]
all_right_nulls = [True, False]
all_chunk_sizes = [0, 3, 10]
all_mpj = [1, 2]
all_swap_tables = [True, False]

from dataclasses import dataclass
from dataclasses_json import dataclass_json
from marshmallow import Schema
from marshmallow_enum import EnumField

@dataclass_json
@dataclass
class JoinTestConfiguration(Schema):
    left_data_type: DataType = EnumField(DataType)
    right_data_type: DataType = EnumField(DataType)
    predicate_condition: PredicateCondition = EnumField(PredicateCondition)
    join_mode: JoinMode = EnumField(JoinMode)
    left_table_size: int
    right_table_size: int
    left_reference_segment: ReferenceSegment = EnumField(ReferenceSegment)
    right_reference_segment: ReferenceSegment = EnumField(ReferenceSegment)
    left_null: bool
    right_null: bool
    chunk_size: int
    mpj: int
    swap_tables: bool
    output_file_path: str = ''
        
    @staticmethod
    def get_random():
        # Avoid Joins between String and some other data type
        left_data_type: DataType = random.choice(list(DataType))
        right_data_type: DataType = random.choice(list(DataType))
        
        # XOR
        while ((left_data_type == DataType.String) != (right_data_type == DataType.String)):
            left_data_type: DataType = random.choice(list(DataType))
            right_data_type: DataType = random.choice(list(DataType))
        
        predicate_condition: PredicateCondition = random.choice(list(PredicateCondition))
        join_mode: JoinMode = random.choice(list(JoinMode))
        # only table size != 0
        left_table_size: int = random.choice([x for x in all_left_table_sizes if x != 0])
        right_table_size: int = random.choice([x for x in all_right_table_sizes if x != 0])
        left_reference_segment: ReferenceSegment = random.choice(list(ReferenceSegment))
        right_reference_segment: ReferenceSegment = random.choice(list(ReferenceSegment))
        left_null: bool = random.choice(all_left_nulls)
        right_null: bool = random.choice(all_right_nulls)
        chunk_size: int = random.choice(all_chunk_sizes)
        mpj: int = random.choice(all_mpj)
        swap_table: bool = random.choice(all_swap_tables)

        return JoinTestConfiguration(
            left_data_type, right_data_type,
            predicate_condition, join_mode,
            left_table_size, right_table_size,
            left_reference_segment, right_reference_segment,
            left_null, right_null,
            chunk_size, mpj, swap_table
        )

# Define all test cases

from typing import List
import random

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

import pandas as pd
from collections import namedtuple
import numpy as np
import math

def try_compare(comparison, l, r) -> bool:
    try:
        return comparison(l, r)
    except TypeError:
        if pd.isnull(l) or pd.isnull(r):
            return False
        raise

def join_condition_to_lambda(condition: PredicateCondition):
    if condition == PredicateCondition.Equals:
        return lambda l, r: try_compare(lambda l, r: l == r, l, r)
    if condition == PredicateCondition.NotEquals:
        return lambda l, r: try_compare(lambda l, r: l != r, l, r)
    if condition == PredicateCondition.LessThan:
        return lambda l, r: try_compare(lambda l, r: l < r, l, r)
    if condition == PredicateCondition.LessThanEquals:
        return lambda l, r: try_compare(lambda l, r: l <= r, l, r)
    if condition == PredicateCondition.GreaterThan:
        return lambda l, r: try_compare(lambda l, r: l > r, l, r)
    if condition == PredicateCondition.GreaterThanEquals:
        return lambda l, r: try_compare(lambda l, r: l >= r, l, r)
    
    raise ValueError('Unexpected PredicateCondition: ' + str(condition))

def prepend_column_names(column_names: List[str], prefix: str):
    return ['{0}_{1}'.format(prefix, i) for i in column_names]
    
def join_rows(
    left: pd.Series, right: pd.Series, 
    mode: JoinMode, condition: PredicateCondition, 
    left_column: str, right_column: str
) -> (pd.DataFrame, bool):
    condition_evaluator = join_condition_to_lambda(condition)

    left_value = left[left_column]
    right_value = right[right_column]
    
    if condition_evaluator(left_value, right_value):
        if mode in [JoinMode.Inner, JoinMode.Left, JoinMode.Right, JoinMode.Full]:
            result = pd.concat([left, right])
            return result, True
        
        if mode in [JoinMode.Semi, JoinMode.AntiNullAsTrue, JoinMode.AntiNullAsFalse]:
            return left, True


    if pd.isnull(left).any() or pd.isnull(right).any():
        if mode == JoinMode.AntiNullAsTrue:
            return left, True
        if mode == JoinMode.AntiNullAsFalse:
            return pd.DataFrame({'A' : [np.nan]}), False
            
    return pd.DataFrame({'A' : [np.nan]}), False

tables = {
    'table_left_0' :pd.read_csv(result_table_path + 'join_table_left_0.tbl', sep='|').drop(0),
    'table_right_0' :pd.read_csv(result_table_path + 'join_table_right_0.tbl', sep='|').drop(0),
    'table_left_10' :pd.read_csv(result_table_path + 'join_table_left_10.tbl', sep='|').drop(0),
    'table_right_10' :pd.read_csv(result_table_path + 'join_table_right_10.tbl', sep='|').drop(0),
    'table_left_15' :pd.read_csv(result_table_path + 'join_table_left_15.tbl', sep='|').drop(0),
    'table_right_15' :pd.read_csv(result_table_path + 'join_table_right_15.tbl', sep='|').drop(0),
}

# Manually set expected data types... only differentiate between Strings and Numerical values in Python
for table_name, table in tables.items():
    table['int'] = table['int'].astype('float')
    table['int_null'] = table['int_null'].astype('float')
    table['float'] = table['float'].astype('float')
    table['float_null'] = table['float_null'].astype('float')
    table['long'] = table['long'].astype('float')
    table['long_null'] = table['long_null'].astype('float')
    table['double'] = table['double'].astype('float')
    table['double_null'] = table['double_null'].astype('float')
    table['string'] = table['string'].astype('str').replace('nan', np.nan)
    table['string_null'] = table['string_null'].astype('str').replace('nan', np.nan)
    print(list(table))

def inner_join(
    left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
    output_rows: List[pd.Series] = []
    
    output_columns = left.columns.union(right.columns)
    
    for l_idx, left_row in left.iterrows():
        for r_idx, right_row in right.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.Inner, condition, left_column, right_column)
            if is_match:
                output_rows.append(output)

    return pd.DataFrame.from_records(output_rows, columns=output_columns)

def left_outer_join(
    left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
    
    output_rows: List[pd.Series] = [] 
    outer_rows: List[pd.Series] = [] 

    output_columns = left.columns.union(right.columns)
    #print(output_columns)
    
    for l_idx, left_row in left.iterrows():
        has_match = False
        for r_idx, right_row in right.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.Left, condition, left_column, right_column)
            if is_match:
                has_match = True
                output_rows.append(output)
        if not has_match:
            outer_rows.append(left_row)

    return pd.DataFrame.from_records(output_rows + outer_rows, columns=output_columns)
    
def right_outer_join(
    left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
    
    output_rows: List[pd.Series] = [] 
    outer_rows: List[pd.Series] = [] 

    output_columns = left.columns.union(right.columns)
    #print(output_columns)

    for r_idx, right_row in right.iterrows():
        has_match = False
        for l_idx, left_row in left.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.Right, condition, left_column, right_column)
            if is_match:
                has_match = True
                output_rows.append(output)
        if not has_match:    
            outer_rows.append(right_row)

    return pd.DataFrame.from_records(output_rows + outer_rows, columns=output_columns)

def full_outer_join(
    left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
    
    output_rows: List[pd.Series] = []
    left_outer_rows: List[bool] = [False]*len(left)
    right_outer_rows: List[bool] = [False]*len(right)

    output_columns = left.columns.union(right.columns)
    #print(output_columns)
    
    for l_idx, left_row in left.iterrows():
        has_match = False
        for r_idx, right_row in right.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.Full, condition, left_column, right_column)
            if is_match:
                has_match = True
                output_rows.append(output)
                left_outer_rows[l_idx-1] = True
                right_outer_rows[r_idx-1] = True

    filtered_left = left.loc[left_outer_rows]
    filtered_right = right.loc[right_outer_rows]
    
    print(list(filtered_left))
    print(list(filtered_right))

    output_df = pd.DataFrame.from_records(output_rows, columns=output_columns)
    return output_df.append(filtered_left, ignore_index = True).append(filtered_right, ignore_index = True)

def semi_join(left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
        
    matched_rows: List[bool] = [False]*len(left)
    
    for l_idx, left_row in left.iterrows():
        for r_idx, right_row in right.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.Semi, condition, left_column, right_column)
            if is_match:
                matched_rows[l_idx-1] = True

    return left.loc[matched_rows]

def anti_null_as_true_join(left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
    matched_rows: List[bool] = [True]*len(left)
    
    for l_idx, left_row in left.iterrows():
        for r_idx, right_row in right.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.AntiNullAsTrue, condition, left_column, right_column)
            if is_match:
                matched_rows[l_idx-1] = False

    return left.loc[matched_rows]

def anti_null_as_false_join(left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
        
    matched_rows: List[bool] = [True]*len(left)
    
    for l_idx, left_row in left.iterrows():
        for r_idx, right_row in right.iterrows():
            output, is_match = join_rows(left_row, right_row, JoinMode.AntiNullAsFalse, condition, left_column, right_column)
            if is_match:
                matched_rows[l_idx-1] = False

    return left.loc[matched_rows]

print(len(result_configurations))

def instantiate_join_mode(
    join_mode: JoinMode, left_table: pd.DataFrame, right_table: pd.DataFrame, 
    predicate_condition: PredicateCondition, left_join_column: str, right_join_column: str):

    #print(left_table)
    #print(right_table)
    
    if join_mode == JoinMode.Inner:
        return inner_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    if join_mode == JoinMode.Left:
        return left_outer_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    if join_mode == JoinMode.Right:
        return right_outer_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    if join_mode == JoinMode.Full:
        return full_outer_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    if join_mode == JoinMode.Semi:
        return semi_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    if join_mode == JoinMode.AntiNullAsTrue:
        return anti_null_as_true_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    if join_mode == JoinMode.AntiNullAsFalse:
        return anti_null_as_false_join(left_table, right_table, predicate_condition, left_join_column, right_join_column)
    
    raise ValueError('Unexpected JoinMode: ' + str(join_mode))

def run_configuration(conf):
    left_data_type: DataType = conf.left_data_type
    right_data_type: DataType = conf.right_data_type
    
    predicate_condition: PredicateCondition = conf.predicate_condition
    join_mode: JoinMode = conf.join_mode
    
    left_table_size: int = conf.left_table_size
    right_table_size: int = conf.right_table_size
    
    left_reference_segment: bool = conf.left_reference_segment
    right_reference_segment: bool = conf.right_reference_segment
    
    left_null: bool = conf.left_null
    right_null: bool = conf.right_null
    
    swap_table: bool = conf.swap_tables
    
    left_table = tables['table_left_' + str(left_table_size)].copy()
    right_table = tables['table_right_' + str(right_table_size)].copy()
    
    if swap_table:
        left_table, right_table = right_table, left_table
        left_join_column = 'l_{}'.format(str(right_data_type.value))
        right_join_column = 'r_{}'.format(str(left_data_type.value))
        if right_null:
            left_join_column = left_join_column + '_null'
        if left_null:
            right_join_column = right_join_column + '_null'
    else:
        left_join_column = 'l_{}'.format(str(left_data_type.value))
        right_join_column = 'r_{}'.format(str(right_data_type.value))
        if left_null:
            left_join_column = left_join_column + '_null'
        if right_null:
            right_join_column = right_join_column + '_null'

    left_table.columns = prepend_column_names(left_table.columns, 'l')
    right_table.columns = prepend_column_names(right_table.columns, 'r')
    
    return instantiate_join_mode(join_mode, left_table, right_table, predicate_condition, left_join_column, right_join_column)
    
def append_data_types(table: pd.DataFrame):
    # Cut off table prefix to extract data type
    data_types = [column_name[2:] for column_name in list(table)]
    column_types = pd.DataFrame(dict(zip(list(table), data_types)), index=[0])
    table = pd.concat([column_types, table], ignore_index=True)
    
    return table
    
for conf in result_configurations:
    print(conf)
    result = run_configuration(conf)
    file_name = 'join_result_{}_{}_{}_{}_{}_{}_{}_{}_{}.tbl'.format(
        conf.left_data_type.value, conf.right_data_type.value,
        conf.predicate_condition.value, conf.join_mode.value,
        str(conf.left_table_size), str(conf.right_table_size), 
        str(conf.left_null), str(conf.right_null), str(conf.swap_tables)
    )
    conf.output_file_path = file_name
    table = append_data_types(result)
    table.to_csv(result_table_path + file_name, index=False, sep="|", na_rep='NULL')   
    
import json

json_configs = [json.loads(conf.to_json()) for conf in result_configurations]
with open(result_table_path + 'join_configurations.json', 'w') as outfile:  
    json.dump(json_configs, outfile)

