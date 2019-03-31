#!/usr/bin/env python
import pandas as pd
import numpy as np

result_table_path = '../../resources/test_data/tbl/join_operators/generated_tables/'

base_values = {
    'int': [1337, 1338],
    'float': [1337.0, 1338.0, 1337.7],
    'double': [1337.0, 1338.0, 1337.7, (2**32) + 1337.0],
    'long': [1337, 1338, (2**32) + 1337],
    'string': ['d', 'm', 'p']
}

left_values = {
    'int': [7331, 724],
    'float': [1337.2, 7331.6, 724.3],
    'double': [1337.2, 7331.6, 724.3],
    'long': [7331, 724],
    'string': ['a', 'o', 'r']
}

right_values = {
    'int': [9331, 976],
    'float': [1337.8, 9331.8, 976.1],
    'double': [1337.8, 9331.8, 976.1],
    'long': [9331, 976],
    'string': ['c', 'l', 't']
}

def generate(number_of_values, possible_values, with_null: bool):
    if number_of_values == 0:
        return np.empty([0])
    
    if with_null:
        possible_values = possible_values + ['NULL', 'NULL', 'NULL']
    
    
    remaining_values = max(0, number_of_values - len(possible_values))
    filling_values = np.random.choice(possible_values, remaining_values)
    foo = np.append(possible_values, filling_values)
    np.random.shuffle(foo)
    return foo

type_row = {
    'int': 'int', 'int_null': 'int', 
    'float': 'float', 'float_null': 'float',
    'double': 'double', 'double_null': 'double',
    'long': 'long', 'long_null': 'long',
    'string': 'string', 'string_null': 'string',
}

for table_size in [0, 10, 15]:
    columns = {}
    
    for data_type in ['int', 'float', 'double', 'long', 'string']:
        columns[data_type] = generate(table_size, base_values[data_type] + left_values[data_type], False)
        columns[data_type + '_null'] = generate(table_size, base_values[data_type] + left_values[data_type], True)
    
    table = pd.DataFrame(columns)
    column_types = pd.DataFrame(dict(zip(list(table), list(table))), index=[0])
    table = pd.concat([column_types, table], ignore_index=True)
    table.columns = ['{0}_{1}'.format('l', i) for i in table.columns]

    table.to_csv(result_table_path + 'join_table_left_' + str(table_size) + '.tbl', index=False, sep="|", na_rep='NULL')
    
for table_size in [0, 10, 15]:
    columns = {}
    
    for data_type in ['int', 'float', 'double', 'long', 'string']:
        columns[data_type] = generate(table_size, base_values[data_type] + right_values[data_type], False)
        columns[data_type + '_null'] = generate(table_size, base_values[data_type] + right_values[data_type], True)
    
    table = pd.DataFrame(columns)
    column_types = pd.DataFrame(dict(zip(list(table), list(table))), index=[0])
    table = pd.concat([column_types, table], ignore_index=True)
    table.columns = ['{0}_{1}'.format('r', i) for i in table.columns]
    
    table.to_csv(result_table_path + 'join_table_right_' + str(table_size) + '.tbl', index=False, sep="|", na_rep='NULL')    
