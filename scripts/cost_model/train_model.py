#!/usr/bin/python

import warnings
warnings.filterwarnings('ignore')

def train_cost_model(model_class, data):
	"""Trains a cost model for a single operator"""
	xValues = data['X'].values
	yValues = data['Y'].values
	return model_class.fit(xValues, yValues)