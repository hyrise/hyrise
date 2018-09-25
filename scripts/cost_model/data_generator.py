#!/usr/bin/python

import sys
from faker import Faker
import pandas as pd
import numpy as np
import json

def load_table_specification(file):
	with open(file) as f:
		config = json.load(f)
        return config['table_specifications']

def type_to_function(column_type):
    switcher = {
        'int': int_column,
        'string': string_column,
        'float': float_column,
    }
    return switcher.get(column_type, lambda: "Invalid column type")

def int_column(row_count, distinct_values):
    # change here for other distributions, numpy provides other distributions in np.random
    return np.random.randint(0,distinct_values,size=(row_count, 1))
    
def string_column(row_count, distinct_values):
    fake = Faker()
    distinct_values = np.array([fake.name() for x in range(distinct_values)])
    return np.random.choice(distinct_values, row_count)
    
def float_column(row_count, distinct_values):
    distinct_values = np.array([np.random.random() for x in range(distinct_values)])
    return np.random.choice(distinct_values, row_count)
    
def generate_column(column_name, row_count, column_specification):
    distinct_values = column_specification['distinct_values']
    value_distribution = column_specification['value_distribution']
    column_type = column_specification['type']
    is_sorted = column_specification.get('sorted', False)
    
    column_generator = type_to_function(column_type)
    data = column_generator(row_count, distinct_values)
    if (is_sorted):
        data = np.sort(data)
    return pd.DataFrame(data, columns=[column_name])

# Generate table
def generate_table(table_specification):
    columns = table_specification['columns']
    column_data = {}

    for column_name, column in columns.items():
        generated_column = generate_column(column_name, table_specification['table_size'], column)
        column_data[column_name] = generated_column
        print('Column ' + column_name + ' generated')
        
    return pd.concat(list(column_data.values()), axis=1)

# Write CSV from table
def write_csv(table_name, table):
    table.to_csv(table_name + '.tbl', index=False, sep="|")
    print('CSV written')

def execute(table_specification):
    table = generate_table(table_specification)
    types = {name: column['type'] for name, column in table_specification['columns'].items()}
    table = pd.concat([pd.DataFrame(types, index=[0]), table], ignore_index=True)
    write_csv(table_specification['table_name'], table)
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Table specification json is missing")
        sys.exit(1)

    table_specification = load_table_specification(sys.argv[1])
    for table_specification in table_specifications:
        execute(table_specification)