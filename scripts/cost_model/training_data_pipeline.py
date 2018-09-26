#!/usr/bin/python

import numpy as np
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

def select_features(df, features = []):
	if len(features) == 0:
		return df
	return df[features]

def generate_dummies(df, dummy_columns):
	return pd.get_dummies(df, columns=dummy_columns)

def split_in_train_and_test(df):
	msk = np.random.rand(len(df)) < 0.8
	return df[msk], df[~msk]

def extract_features_and_target(df):
	dfX=df.drop('execution_time_ns', axis=1)
	dfY=df['execution_time_ns']

	return dfX, dfY

def normalize_features(train_data, test_data):
	"""Reads unnormalized training and test data and returns both normalized"""

	mean = train_data.mean(axis=0)
	std = train_data.std(axis=0)

	train_data = (train_data - mean) / std
	test_data = (test_data - mean) / std

	return train_data, test_data

def transform_calibration_results(raw_data, features = [], dummies = [], operators = []):
	"""Transforms raw results from calibration into Dict of DataFrames per operator."""
	if len(operators) > 0:
		raw_data = { k: raw_data[k] for k in operators }

	def transform_single_df(data):
		df = transform_to_dataframe(data)
		df = select_features(df, features)
		df = generate_dummies(df, dummies)
		return extract_features_and_target(df)

	return { operator_type: transform_single_df(values) for operator_type, values in raw_data.items()}