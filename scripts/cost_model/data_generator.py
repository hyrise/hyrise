#!/usr/bin/python

import sys
from faker import Faker
import pandas as pd
import numpy as np
import json

import random


class DataGenerator:
    table_specifications = []
    column_specification = []

    def __init__(self, file):
        with open(file) as f:
            config = json.load(f)
            self.table_specifications = config['table_specifications']
            self.column_specification = config['columns']

    def type_to_function(self, column_type):
        switcher = {
            'int': self.int_column,
            'string': self.string_column,
            'float': self.float_column,
            'long': self.long_column,
            'double': self.double_column,
        }
        return switcher.get(column_type, lambda: "Invalid column type")

    @staticmethod
    def key_column(row_count):
        return np.array(range(row_count))

    @staticmethod
    def foreign_key_column(row_count):
        return np.array(range(row_count / 10))

    @staticmethod
    def int_column(row_count, distinct_values):
        distinct_values = random.sample(range(0, 10000000), distinct_values)
        return np.random.choice(distinct_values, row_count)

    @staticmethod
    def long_column(row_count, distinct_values):
        return DataGenerator.int_column(row_count, distinct_values)

    @staticmethod
    def string_column(row_count, distinct_values):
        faker = Faker()
        distinct_values = np.array([faker.password(length=10, special_chars=False, digits=False,
                                                  upper_case=True, lower_case=False) for _ in range(distinct_values)])
        return np.random.choice(distinct_values, row_count)

    @staticmethod
    def float_column(row_count, num_distinct_values):
        distinct_values = np.array([np.random.random() for _ in range(num_distinct_values)])
        return np.random.choice(distinct_values, row_count)

    @staticmethod
    def double_column(row_count, distinct_values):
        return DataGenerator.float_column(row_count, distinct_values)

    def generate_column(self, row_count, column_specification):
        column_name = column_specification['column_name']
        distinct_values = column_specification.get('distinct_values', max(int(row_count/100), 100))
        value_distribution = column_specification.get('value_distribution', "uniform")
        column_type = column_specification.get('type', "int")
        is_sorted = column_specification.get('sorted', False)

        if column_name == 'column_pk':
            return pd.DataFrame(self.key_column(row_count), columns=[column_name])

        if column_name == 'foreign_key':
            return pd.DataFrame(self.foreign_key_column(row_count), columns=[column_name])

        column_generator = self.type_to_function(column_type)
        data = column_generator(row_count, distinct_values)
        if is_sorted:
            data = np.sort(data, axis=None)
        return pd.DataFrame(data, columns=[column_name])

    def generate_table(self, table_specification):
        columns = self.column_specification
        column_data = {}

        for column in columns:
            column_name = column['column_name']
            generated_column = self.generate_column(table_specification['table_size'], column)
            column_data[column_name] = generated_column
            print('Column ' + column_name + ' generated')

        return pd.concat(list(column_data.values()), axis=1)

    @staticmethod
    def write_csv(table_path, table):
        table.to_csv(table_path, index=False, sep="|")
        print('CSV written')

    def execute(self):
        type_row = {column['column_name']: column['type'] for column in self.column_specification}

        for table_specification in self.table_specifications:
            table = self.generate_table(table_specification)
            table = pd.concat([pd.DataFrame(type_row, index=[0]), table], ignore_index=True)
            self.write_csv(table_specification['table_path'], table)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Table specification json is missing")
        sys.exit(1)

    dg = DataGenerator(sys.argv[1])
    dg.execute()
