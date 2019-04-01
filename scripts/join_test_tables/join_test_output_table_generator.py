#!/usr/bin/env python

import pandas as pd
from collections import namedtuple
import numpy as np
import math
import json
from typing import List
from join_test_configuration import DataType, PredicateCondition, JoinMode, ReferenceSegment, JoinTestConfiguration

result_table_path = '../../resources/test_data/tbl/join_operators/generated_tables/'

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
#for table_name, table in tables.items():
#    table['int'] = table['int'].astype('float')
#    table['int_null'] = table['int_null'].astype('float')
#    table['float'] = table['float'].astype('float')
#    table['float_null'] = table['float_null'].astype('float')
#    table['long'] = table['long'].astype('float')
#    table['long_null'] = table['long_null'].astype('float')
#    table['double'] = table['double'].astype('float')
#    table['double_null'] = table['double_null'].astype('float')
#    table['string'] = table['string'].astype('str').replace('nan', np.nan)
#    table['string_null'] = table['string_null'].astype('str').replace('nan', np.nan)
#    print(list(table))

def inner_join(
    left: pd.DataFrame, right: pd.DataFrame, 
    condition: PredicateCondition,
    left_column: str, right_column: str):
    output_rows: List[pd.Series] = []
    
    output_columns = left.columns.append(right.columns)
    #print(output_columns)
    
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

    output_columns = left.columns.append(right.columns)
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

    output_columns = left.columns.append(right.columns)
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

    output_columns = left.columns.append(right.columns)
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
    
    #print(list(filtered_left))
    #print(list(filtered_right))

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

    result = left.loc[matched_rows]
    #print(list(result))
    return result

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
    
    left_nullable: bool = conf.left_nullable
    right_nullable: bool = conf.right_nullable
    
    swap_table: bool = conf.swap_tables
    
    left_table = tables['table_left_' + str(left_table_size)].copy()
    right_table = tables['table_right_' + str(right_table_size)].copy()
    
    if swap_table:
        left_table, right_table = right_table, left_table
        left_join_column = 'r_{}'.format(str(right_data_type.value).lower())
        right_join_column = 'l_{}'.format(str(left_data_type.value).lower())
        if right_nullable:
            left_join_column = left_join_column + '_null'
        if left_nullable:
            right_join_column = right_join_column + '_null'
    else:
        left_join_column = 'l_{}'.format(str(left_data_type.value).lower())
        right_join_column = 'r_{}'.format(str(right_data_type.value).lower())
        if left_nullable:
            left_join_column = left_join_column + '_null'
        if right_nullable:
            right_join_column = right_join_column + '_null'

#    left_table.columns = prepend_column_names(left_table.columns, 'l')
#    right_table.columns = prepend_column_names(right_table.columns, 'r')
    
    return instantiate_join_mode(join_mode, left_table, right_table, predicate_condition, left_join_column, right_join_column)
    
def append_data_types(table: pd.DataFrame):
    # Cut off table prefix to extract data type
    data_types = [column_name[2:] for column_name in list(table)]
    column_types = pd.DataFrame(dict(zip(list(table), data_types)), index=[0])
    table = pd.concat([column_types, table], ignore_index=True)
    
    return table
   
with open(result_table_path + 'join_configurations.json', 'r') as f:
	json_confs = json.load(f)
	result_configurations = [JoinTestConfiguration.from_json(json.dumps(conf)) for conf in json_confs]

#json.loads(conf.to_json())

print(len(result_configurations))

for conf in result_configurations:
    print(conf)
    print(result_table_path + conf.output_file_path)
    result = run_configuration(conf)
    table = append_data_types(result)
    table.to_csv(result_table_path + conf.output_file_path, index=False, sep="|", na_rep='null')

