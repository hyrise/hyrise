#!/usr/bin/env python

import numpy as np
import json
from typing import List
from join_test_configuration import DataType, PredicateCondition, JoinMode, ReferenceSegment, JoinTestConfiguration
import csv

result_table_path = '../../resources/test_data/tbl/join_operators/generated_tables/'

cast = {
    'int': lambda x: int(x),
    'long': lambda x: int(x),
    'float': lambda x: np.float32(x),
    'double': lambda x: np.float64(x),
    'string': lambda x: str(x)
}


class Table:
    def __init__(self, column_names, column_data_types, column_nullability):
        self.rows = []
        self.column_names = column_names
        self.column_data_types = column_data_types
        self.column_nullability = column_nullability

    def append(self, row):
        converted_values = []
        for value, data_type, is_nullable in zip(row, self.column_data_types, self.column_nullability):
            if value == 'null' or value is None:
                assert is_nullable
                converted_values.append(None)
            else:
                converted_values.append(cast[data_type](value))
        self.rows.append(converted_values)


def load_table(path):
    with open(path) as csvfile:
        csvreader = csv.reader(csvfile, delimiter="|", quotechar="\"")

        column_names = next(csvreader)
        column_data_types_and_nullability = next(csvreader)
        column_data_types = [v.split("_")[0] for v in column_data_types_and_nullability]
        column_nullability = [True if len(v.split("_")) == 2 else False for v in column_data_types_and_nullability]

        table = Table(column_names, column_data_types, column_nullability)

        for row in csvreader:
            table.append(row)

        return table


def write_table(path: str, table: Table):
    with open(path, "w") as file:
        file.write("|".join(table.column_names) + "\n")

        column_data_types_and_nullability = []
        for data_type, nullable in zip(table.column_data_types, table.column_nullability):
            if nullable:
                column_data_types_and_nullability.append(data_type + "_null")
            else:
                column_data_types_and_nullability.append(data_type)

        file.write("|".join(column_data_types_and_nullability) + "\n")

        for row in table.rows:
            row_with_nulls = ["null" if v is None else str(v) for v in row]
            file.write("|".join(row_with_nulls) + "\n")


def try_compare(comparison, l, r):
    if l is None or r is None:
        return None
    return comparison(l, r)


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
    left_row: List, right_row: List,
    mode: JoinMode, condition: PredicateCondition, 
    left_column: int, right_column: int
) -> bool:
    condition_evaluator = join_condition_to_lambda(condition)

    left_value = left_row[left_column]
    right_value = right_row[right_column]
    
    return condition_evaluator(left_value, right_value)


def inner_join(
    left: Table, right: Table,
    condition: PredicateCondition,
    left_column_name: str, right_column_name: str):

    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names + right.column_names
    output_column_data_types = left.column_data_types + right.column_data_types
    output_column_nullability = left.column_nullability + right.column_nullability

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)
    
    for l_idx, left_row in enumerate(left.rows):
        for r_idx, right_row in enumerate(right.rows):
            if join_rows(left_row, right_row, JoinMode.Inner, condition, left_column, right_column):
                output_table.append(left_row + right_row)

    return output_table


def left_outer_join(
    left: Table, right: Table,
    condition: PredicateCondition,
    left_column_name: str, right_column_name: str):

    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names + right.column_names
    output_column_data_types = left.column_data_types + right.column_data_types
    output_column_nullability = left.column_nullability + [True] * len(right.column_nullability)

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)

    for l_idx, left_row in enumerate(left.rows):
        has_match = False
        for r_idx, right_row in enumerate(right.rows):
            if join_rows(left_row, right_row, JoinMode.Left, condition, left_column, right_column):
                has_match = True
                output_table.append(left_row + right_row)
        if not has_match:
            output_table.append(left_row + [None] * len(right.column_names))

    return output_table


def right_outer_join(
        left: Table, right: Table,
        condition: PredicateCondition,
        left_column_name: str, right_column_name: str):
    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names + right.column_names
    output_column_data_types = left.column_data_types + right.column_data_types
    output_column_nullability = [True] * len(left.column_nullability) + right.column_nullability

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)

    for r_idx, right_row in enumerate(right.rows):
        has_match = False
        for l_idx, left_row in enumerate(left.rows):
            if join_rows(left_row, right_row, JoinMode.Left, condition, left_column, right_column):
                has_match = True
                output_table.append(left_row + right_row)
        if not has_match:
            output_table.append([None] * len(left.column_names) + right_row)

    return output_table


def full_outer_join(
        left: Table, right: Table,
        condition: PredicateCondition,
        left_column_name: str, right_column_name: str):
    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names + right.column_names
    output_column_data_types = left.column_data_types + right.column_data_types
    output_column_nullability = [True] * len(left.column_nullability) + [True] * len(right.column_nullability)

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)

    left_matches: List[bool] = [False]*len(left.rows)
    right_matches: List[bool] = [False]*len(right.rows)
    
    for l_idx, left_row in enumerate(left.rows):
        for r_idx, right_row in enumerate(right.rows):
            if join_rows(left_row, right_row, JoinMode.Full, condition, left_column, right_column):
                output_table.append(left_row + right_row)
                left_matches[l_idx] = True
                right_matches[r_idx] = True

    for l_idx, l_match in enumerate(left_matches):
        if not l_match:
            output_table.append(left.rows[l_idx] + [None] * len(right.column_names))

    for r_idx, r_match in enumerate(right_matches):
        if not r_match:
            output_table.append([None] * len(left.column_names) + right.rows[r_idx])

    return output_table


def semi_join(
        left: Table, right: Table,
        condition: PredicateCondition,
        left_column_name: str, right_column_name: str):
    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names
    output_column_data_types = left.column_data_types
    output_column_nullability = left.column_nullability

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)

    matched_rows: List[bool] = [False]*len(left.rows)

    for l_idx, left_row in enumerate(left.rows):
        for r_idx, right_row in enumerate(right.rows):
            if join_rows(left_row, right_row, JoinMode.Semi, condition, left_column, right_column):
                matched_rows[l_idx] = True

    for l_idx, l_match in enumerate(matched_rows):
        if l_match:
            output_table.append(left.rows[l_idx])

    return output_table


def anti_null_as_true_join(
        left: Table, right: Table,
        condition: PredicateCondition,
        left_column_name: str, right_column_name: str):
    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names
    output_column_data_types = left.column_data_types
    output_column_nullability = left.column_nullability

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)

    matched_rows: List[bool] = [False]*len(left.rows)

    for l_idx, left_row in enumerate(left.rows):
        for r_idx, right_row in enumerate(right.rows):
            match = join_rows(left_row, right_row, JoinMode.Semi, condition, left_column, right_column)
            if match or match is None:
                matched_rows[l_idx] = True

    for l_idx, l_match in enumerate(matched_rows):
        if not l_match:
            output_table.append(left.rows[l_idx])

    return output_table


def anti_null_as_false_join(
        left: Table, right: Table,
        condition: PredicateCondition,
        left_column_name: str, right_column_name: str):
    left_column = left.column_names.index(left_column_name)
    right_column = right.column_names.index(right_column_name)

    output_column_names = left.column_names
    output_column_data_types = left.column_data_types
    output_column_nullability = left.column_nullability

    output_table = Table(output_column_names, output_column_data_types, output_column_nullability)

    matched_rows: List[bool] = [False]*len(left.rows)

    for l_idx, left_row in enumerate(left.rows):
        for r_idx, right_row in enumerate(right.rows):
            match = join_rows(left_row, right_row, JoinMode.Semi, condition, left_column, right_column)
            if match:
                matched_rows[l_idx] = True

    for l_idx, l_match in enumerate(matched_rows):
        if not l_match:
            output_table.append(left.rows[l_idx])

    return output_table


def instantiate_join_mode(
    join_mode: JoinMode, left_table: Table, right_table: Table,
    predicate_condition: PredicateCondition, left_join_column: str, right_join_column: str):

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

    left_nullable: bool = conf.left_nullable
    right_nullable: bool = conf.right_nullable
    
    swap_table: bool = conf.swap_tables
    
    left_table = tables['table_left_' + str(left_table_size)]
    right_table = tables['table_right_' + str(right_table_size)]
    
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


    return instantiate_join_mode(join_mode, left_table, right_table, predicate_condition, left_join_column, right_join_column)


tables = {
    'table_left_0' :load_table(result_table_path + 'join_table_left_0.tbl'),
    'table_right_0' :load_table(result_table_path + 'join_table_right_0.tbl'),
    'table_left_10' :load_table(result_table_path + 'join_table_left_10.tbl'),
    'table_right_10' :load_table(result_table_path + 'join_table_right_10.tbl'),
    'table_left_15' :load_table(result_table_path + 'join_table_left_15.tbl'),
    'table_right_15' :load_table(result_table_path + 'join_table_right_15.tbl'),
}

with open(result_table_path + 'join_configurations.json', 'r') as f:
    json_confs = json.load(f)
    result_configurations = [JoinTestConfiguration.from_json(json.dumps(conf)) for conf in json_confs]

for conf in result_configurations:
    #print(conf)
    print(result_table_path + conf.output_file_path)

    result_table = run_configuration(conf)
    #print(len(result_table.rows))

    write_table(result_table_path + conf.output_file_path, result_table)

