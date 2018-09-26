#!/usr/bin/python

from sklearn.model_selection import cross_val_score
from sklearn.metrics import explained_variance_score
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

import pandas as pd
import numpy as np
from math import sqrt

import warnings
warnings.filterwarnings('ignore')

def cross_validation(model_class, data, scores = [
	'explained_variance', 'r2', 'neg_mean_absolute_error', 'neg_mean_squared_error', 
	'neg_mean_squared_log_error', 'neg_median_absolute_error']):

	a = np.array(data['Y'])
	print('Mean: ' + str(np.mean(a)))
	print('Median ' + str(np.median(a)))

	for score in scores:
		result = cross_val_score(model_class, data['X'], data['Y'], scoring=score, cv=10)
		print(model_class.__class__.__name__ + " - " + score + ":")
		print(str( np.array(result).mean() ))

def regression_metrics(model, data):
	xValues = data['X'].values
	yValues = data['Y'].values

	print(model.__class__.__name__)

	y_predicted = [ model.predict(x.reshape(1, -1)) for x in xValues ]
	print('Explained Variance Score: ' + str(explained_variance_score(yValues, y_predicted)))
	print('Mean Absolute Error: ' + str(mean_absolute_error(yValues, y_predicted)))
	print('Mean Squared Error: ' + str(mean_squared_error(yValues, y_predicted)))
	print('Root Mean Squared Error: ' + str(sqrt(mean_squared_error(yValues, y_predicted))))
	print('R2 Score: ' + str(r2_score(yValues, y_predicted)))

def feature_importances(model, column_names):
	return pd.DataFrame([model.feature_importances_], columns=column_names)

def mean_absolute_percentage_error(y_true, y_pred):
	y_true, y_pred = np.array(y_true), np.array(y_pred)
	return np.mean(np.abs((y_true - y_pred) / y_true))

def print_predictions(df, model, columns, label):
	import matplotlib.pyplot as plt
	
	def predict(selectivity, columns):
		row = df.iloc[0].copy()

		for name, column in row.iteritems():
			if name in columns:
				row[name] = 1
			else:
				row[name] = 0

		row['output_selectivity'] = selectivity
		row_features = row.drop('execution_time_ns').values
		row_features = row_features.reshape(1, -1)
		predicted = model.predict(row_features)
		return predicted[0]

	predictions = {s/100.0: predict(s / 100.0, columns) for s in range(100)}
	plt.plot(predictions.keys(), predictions.values(), label=label)

def print_raw_data(df, columns, label):
	import matplotlib.pyplot as plt
	for column in columns:
		df = df[df[column] == 1]
	grouped = df.groupby(['output_selectivity'], as_index=False)['execution_time_ns'].mean()
	plt.plot(grouped['output_selectivity'], grouped['execution_time_ns'], label=label, linestyle="--")