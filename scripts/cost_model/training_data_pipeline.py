#!/usr/bin/python

import pandas as pd
import json

import warnings
warnings.filterwarnings('ignore')

def load_json_results(path):
	"""Reads calibration result file and returns Dict[Operator -> Array]"""
	with open(path) as json_data:
		return json.load(json_data)['operators']

def transform_to_dataframe(data):
	return pd.DataFrame(data)

def generate_dummies(df, dummy_columns):
	return pd.get_dummies(df, columns=dummy_columns)

def select_features(df, features = []):
	if len(features) == 0:
		return df
	return df[features]

def split_data(df):
	dfX=df.drop('execution_time_ns', axis=1)
	dfY=df['execution_time_ns']

	return {'X': dfX, 'Y': dfY}

def transform_calibration_results(raw_data, features = [], dummies = [], operators = []):
	"""Transforms raw results from calibration into Dict of DataFrames per operator."""
	if len(operators) > 0:
		raw_data = { k: raw_data[k] for k in operators }

	def transform_single_df(data):
		df = transform_to_dataframe(data)
		df = select_features(df, features)
		df = generate_dummies(df, dummies)
		return split_data(df)

	return { operator_type: transform_single_df(values) for operator_type, values in raw_data.items()}