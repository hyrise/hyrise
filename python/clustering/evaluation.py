from .pqp_input_parser import PQPInputParser

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sklearn.metrics
from matplotlib.ticker import MaxNLocator

def histogram(values, bins):
    def elements_between(low, high, upper_exclusive=True):
        if upper_exclusive:        
            filtered = filter(lambda x: low <= x and x < high, values)
        else:
            filtered = filter(lambda x: low <= x and x <= high, values)
            
        return len(list(filtered))
    
    return [elements_between(low, high ) for low, high in zip(bins, bins[1:])]

def evaluate_join_step(m, ground_truth_path, clustering_columns, sorting_column, dimension_cardinalities, step, sided=True):
  side = None
  if step == "BUILD_SIDE_MATERIALIZING" or step == "BUILDING":
      side = "BUILD"
  elif step == "PROBE_SIDE_MATERIALIZING" or step == "PROBING":
      side = "PROBE"
  
  
  if step == "ALL":
      measured_column = "RUNTIME_NS"
      estimate_column = "RUNTIME_ESTIMATE"
  else:
      measured_column = step + "_NS"
      estimate_column = "ESTIMATE_" + step
  
  m.estimate_total_runtime(clustering_columns, sorting_column, dimension_cardinalities)
  
  #assert len(m.join_estimates[m.join_estimates['ESTIMATE_BUILD_SIDE_MATERIALIZING'] < 0]) == 0, "not all runtimes computed"
  #assert len(m.join_estimates[m.join_estimates['ESTIMATE_PROBE_SIDE_MATERIALIZING'] < 0]) == 0, "not all runtimes computed"
  #assert len(m.join_estimates[m.join_estimates['ESTIMATE_CLUSTERING'] < 0]) == 0, "not all runtimes computed"
  #assert len(m.join_estimates[m.join_estimates['ESTIMATE_BUILDING'] < 0]) == 0, "not all runtimes computed"
  #assert len(m.join_estimates[m.join_estimates['ESTIMATE_PROBING'] < 0]) == 0, "not all runtimes computed"
  #assert len(m.join_estimates[m.join_estimates['ESTIMATE_OUTPUT_WRITING'] < 0]) == 0, "not all runtimes computed"
  
  assert len(m.join_estimates[m.join_estimates['RUNTIME_ESTIMATE'] == -1]) == 0, "not all runtimes computed"
  negative_estimates = m.join_estimates[m.join_estimates['RUNTIME_ESTIMATE'] < 0]
  if len(negative_estimates) > 0:
    print(f"There are {len(negative_estimates)} joins with estimated negative run time. This is bad.")
    print(negative_estimates)
    m.join_estimates.loc[negative_estimates.index, 'RUNTIME_ESTIMATE'] = 1

  clustered_stats_parser = PQPInputParser("tpch", ground_truth_path)
  clustered_stats_parser.load_statistics()
  clustered_joins = clustered_stats_parser.get_joins()
  clustered_joins = clustered_joins.sort_values(['QUERY_HASH', 'DESCRIPTION'])
  
  assert len(m.joins) == len(clustered_joins), "Different number of joins."
  
  frequencies = clustered_stats_parser.get_query_frequencies()
  current_sum = m.joins.apply(lambda x: x[measured_column] * frequencies[x['QUERY_HASH']], axis=1).sum()
  estimate_sum = int(m.join_estimates.apply(lambda x: x[estimate_column] * frequencies[x['QUERY_HASH']], axis=1).sum())
  new_sum = clustered_joins.apply(lambda x: x[measured_column] * frequencies[x['QUERY_HASH']], axis=1).sum()
      
  if side is not None and sided:
      clustered_joins = clustered_joins[clustered_joins[f"{side}_TABLE"] == m.table_name]
      print(f"{len(clustered_joins)} joins with {table_name} as the {side} side")
  elif step == "ALL" and sided:
      probe_side_joins = clustered_joins[f"PROBE_TABLE"] == m.table_name
      build_side_joins = clustered_joins[f"BUILD_TABLE"] == m.table_name
      clustered_joins = clustered_joins[probe_side_joins | build_side_joins]
      print(f"{len(clustered_joins)} joins with {m.table_name} as probe or build side")
  
  m.join_estimates.sort_values(['QUERY_HASH', 'DESCRIPTION'], inplace=True)
  estimates = m.join_estimates.loc[clustered_joins.index]
  assert len(estimates) == len(clustered_joins), "lengths do not match"

  if (side is not None or step == "ALL") and sided:
      current_sum = m.joins.loc[clustered_joins.index].apply(lambda x: x[measured_column] * frequencies[x['QUERY_HASH']], axis=1).sum()
      estimate_sum = int(estimates.apply(lambda x: x[estimate_column] * frequencies[x['QUERY_HASH']], axis=1).sum())
      new_sum = clustered_joins.apply(lambda x: x[measured_column] * frequencies[x['QUERY_HASH']], axis=1).sum()

  result = pd.DataFrame()
  if side is not None and sided:
      result['COLUMN_NAME'] = np.array(clustered_joins[f"{side}_COLUMN"])
  elif step == "ALL" and sided:
      probe_build_column_name = clustered_joins.apply(lambda x: f"{x['PROBE_COLUMN']}/{x['BUILD_COLUMN']}", axis=1)
      result['COLUMN_NAME'] = np.array(probe_build_column_name)

  result['DESCRIPTION1'] = np.array(estimates['DESCRIPTION'])
  result['DESCRIPTION2'] = np.array(clustered_joins['DESCRIPTION'])
  result['QUERY_HASH1'] = np.array(estimates['QUERY_HASH'])
  result['QUERY_HASH2'] = np.array(clustered_joins['QUERY_HASH'])
  result['RUNTIME_BASE'] = np.array(estimates[measured_column])
  #result['RUNTIME_ESTIMATE'] = np.array(estimates[f"ESTIMATE_{step}"], dtype=np.int64)
  result['RUNTIME_ESTIMATE'] = np.array(estimates[estimate_column], dtype=np.int64)
  result['RUNTIME_CLUSTERED'] = np.array(clustered_joins[measured_column])
  
  
  # make sure we match all operators
  matches = result.apply(lambda row: row['DESCRIPTION1'] == row['DESCRIPTION2'] and row['QUERY_HASH1'] == row['QUERY_HASH2'], axis=1)
  assert matches.all(), "not all rows match"
  
  result['TOTAL_ERROR'] = result['RUNTIME_CLUSTERED'] - result['RUNTIME_ESTIMATE']
  result['RELATIVE_ERROR'] = result['RUNTIME_CLUSTERED'] / result['RUNTIME_ESTIMATE']
  if step == "CLUSTERING":
      result['RELATIVE_ERROR'].fillna(1, inplace=True)
  
  # add some nicely formatted columns to display the data
  result['RUNTIME_BASE_MS'] = result['RUNTIME_BASE'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))
  result['RUNTIME_ESTIMATE_MS'] = result['RUNTIME_ESTIMATE'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))
  result['RUNTIME_CLUSTERED_MS'] = result['RUNTIME_CLUSTERED'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))
  result['TOTAL_ERROR_MS'] = result['TOTAL_ERROR'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))

  return result

def evaluate_scans(m, ground_truth_path, clustering_columns, sorting_column, dimension_cardinalities):
  m.estimate_total_runtime(clustering_columns, sorting_column, dimension_cardinalities)

  assert len(m.scan_estimates[m.scan_estimates['RUNTIME_ESTIMATE'] == -1]) == 0, "not all runtimes computed"
  negative_estimates = m.scan_estimates[m.scan_estimates['RUNTIME_ESTIMATE'] < 0]
  if len(negative_estimates) > 0:
    print(f"There are {len(negative_estimates)} negative scan estimates. This is bad.")
    print(negative_estimates)
    m.scan_estimates.loc[negative_estimates.index, 'RUNTIME_ESTIMATE'] = 1
  
  path = f"{ground_truth_path}/table_scans.csv"
  clustered_scans = pd.read_csv(path, sep='|')
  clustered_scans = clustered_scans[clustered_scans['TABLE_NAME'] == m.table_name]
  clustered_scans = clustered_scans.sort_values(['QUERY_HASH', 'DESCRIPTION'])
  
  assert len(clustered_scans) == len(m.scan_estimates), "Different number of scans."
  
  m.scan_estimates.sort_values(['QUERY_HASH', 'DESCRIPTION'], inplace=True)
  result = pd.DataFrame()
  result['QUERY_HASH'] = np.array(clustered_scans['QUERY_HASH'])
  result['rti'] = np.array(m.scan_estimates['time_per_input_row'])
  result['COLUMN_NAME'] = np.array(clustered_scans['COLUMN_NAME'])
  result['DESCRIPTION1'] = np.array(m.scan_estimates['DESCRIPTION'])
  result['DESCRIPTION2'] = np.array(clustered_scans['DESCRIPTION'])
  result['QUERY_HASH1'] = np.array(m.scan_estimates['QUERY_HASH'])
  result['QUERY_HASH2'] = np.array(clustered_scans['QUERY_HASH'])
  result['RUNTIME_BASE'] = np.array(m.scan_estimates['RUNTIME_NS'])
  result['RUNTIME_ESTIMATE'] = np.array(m.scan_estimates.apply(lambda row: row['RUNTIME_ESTIMATE'] / m.query_frequency(row['QUERY_HASH']), axis=1), dtype=np.int64)
  result['RUNTIME_CLUSTERED'] = np.array(clustered_scans['RUNTIME_NS'])
  
  # make sure we match all operators
  matches = result.apply(lambda row: row['DESCRIPTION1'] == row['DESCRIPTION2'] and row['QUERY_HASH1'] == row['QUERY_HASH2'], axis=1)
  assert matches.all(), "not all rows match"
  
  result['TOTAL_ERROR'] = result['RUNTIME_CLUSTERED'] - result['RUNTIME_ESTIMATE']
  result['RELATIVE_ERROR'] = result['RUNTIME_CLUSTERED'] / result['RUNTIME_ESTIMATE']
  
  return result

def evaluate_aggregates(m, ground_truth_path, clustering_columns, sorting_column, dimension_cardinalities):
  m.estimate_total_runtime(clustering_columns, sorting_column, dimension_cardinalities)

  assert len(m.aggregate_estimates[m.aggregate_estimates['RUNTIME_ESTIMATE'] == -1]) == 0, "not all runtimes computed"
  negative_estimates = m.aggregate_estimates[m.aggregate_estimates['RUNTIME_ESTIMATE'] < 0]
  if len(negative_estimates) > 0:
    print(f"There are {len(negative_estimates)} negative aggregate estimates. This is bad.")
    print(negative_estimates)
    m.scan_estimates.loc[negative_estimates.index, 'RUNTIME_ESTIMATE'] = 1
  
  ground_truth_parser = PQPInputParser("tpch", ground_truth_path)
  ground_truth_parser.load_statistics()    
  clustered_aggregates = ground_truth_parser.get_aggregates()
  if False:
    clustered_aggregates = clustered_aggregates[clustered_aggregates['TABLE_NAME'] == m.table_name]
  clustered_aggregates = clustered_aggregates.sort_values(['QUERY_HASH', 'DESCRIPTION'])

  
  assert len(clustered_aggregates) == len(m.aggregate_estimates), "Different number of aggregates."
  
  m.aggregate_estimates.sort_values(['QUERY_HASH', 'DESCRIPTION'], inplace=True)
  result = pd.DataFrame()
  result['QUERY_HASH'] = np.array(clustered_aggregates['QUERY_HASH'])
  result['COLUMN_NAME'] = np.array(clustered_aggregates['COLUMN_NAME'])
  result['DESCRIPTION1'] = np.array(m.aggregate_estimates['DESCRIPTION'])
  result['DESCRIPTION2'] = np.array(clustered_aggregates['DESCRIPTION'])
  result['QUERY_HASH1'] = np.array(m.aggregate_estimates['QUERY_HASH'])
  result['QUERY_HASH2'] = np.array(clustered_aggregates['QUERY_HASH'])
  result['RUNTIME_BASE'] = np.array(m.aggregate_estimates['RUNTIME_NS'], dtype=np.int64)
  result['RUNTIME_ESTIMATE'] = np.array(m.aggregate_estimates.apply(lambda row: row['RUNTIME_ESTIMATE'] / m.query_frequency(row['QUERY_HASH']), axis=1), dtype=np.int64)
  result['RUNTIME_CLUSTERED'] = np.array(clustered_aggregates['RUNTIME_NS'], dtype=np.int64)
  

  # make sure we match all operators
  matches = result.apply(lambda row: row['DESCRIPTION1'] == row['DESCRIPTION2'] and row['QUERY_HASH1'] == row['QUERY_HASH2'], axis=1)
  assert matches.all(), "not all rows match"
  
  result['TOTAL_ERROR'] = result['RUNTIME_CLUSTERED'] - result['RUNTIME_ESTIMATE']
  result['RELATIVE_ERROR'] = result['RUNTIME_CLUSTERED'] / result['RUNTIME_ESTIMATE']
  
  # add some nicely formatted columns to display the data
  result['RUNTIME_BASE_MS'] = result['RUNTIME_BASE'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))
  result['RUNTIME_ESTIMATE_MS'] = result['RUNTIME_ESTIMATE'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))
  result['RUNTIME_CLUSTERED_MS'] = result['RUNTIME_CLUSTERED'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))
  result['TOTAL_ERROR_MS'] = result['TOTAL_ERROR'].apply(lambda x: "{:,}".format(np.int64(x / 1e6)))

  return result

def plot_scan_errors(estimation_errors, old_estimation_errors, query_frequencies, save_path=None):
  bins = [0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.1, 1.3, 1.5, 1.8, 2.1, 2.5, 5, 10, 50, 500, 1e21]
  plot_relative_errors(estimation_errors, old_estimation_errors, query_frequencies, "scan", bins, save_path)


def plot_join_errors(estimation_errors, old_estimation_errors, query_frequencies, save_path=None):
  bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.1, 1.3, 1.5, 1.8, 2.1, 2.5, 5, 10, 30, 500, 1e21]
  plot_relative_errors(estimation_errors, old_estimation_errors, query_frequencies, "join", bins, save_path)

def plot_aggregate_errors(estimation_errors, old_estimation_errors, query_frequencies, save_path=None):
  bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.1, 1.3, 1.5, 1.8, 2.1, 2.5, 5, 10, 30, 500, 1e21]
  plot_relative_errors(estimation_errors, old_estimation_errors, query_frequencies, "aggregate", bins, save_path)

def plot_relative_errors(estimation_errors, old_estimation_errors, query_frequencies, operator, bins, save_path):
  def compute_histogram(errors):
    all_factors = []
    for _, row in errors.iterrows():
        frequency = query_frequencies[row['QUERY_HASH1']]
        for _ in range(frequency):
            all_factors.append(row['RELATIVE_ERROR'])
    return histogram(all_factors, bins)

  values = compute_histogram(estimation_errors)
  if old_estimation_errors is not None:
    old_values = compute_histogram(old_estimation_errors)
  else:
    old_values = None

  x = np.arange(len(values))
  labels = list([f"[{low},{high})" for low, high in zip(bins, bins[1:])])

  fig, ax = plt.subplots()
  ax.set_xticks(x)
  ax.set_xticklabels(labels)
  ax.set_xlabel("Relative estimation error")
  ax.set_ylabel(f"Number of {operator}s")
  if old_values is not None:
    ax.bar(x-0.20, old_values, label='old', width=0.4)
  ax.bar(x+0.20, values, label='new', width=0.4)
  ax.get_yaxis().set_major_locator(MaxNLocator(integer=True))
  plt.xticks(rotation=90)
  plt.legend()

  if save_path is not None:
    plt.savefig(save_path, bbox_inches='tight')

  plt.show()


def get_percentage_bounds(results, percentage_bounds):
  percentages = []

  for bound in percentage_bounds:
      mi = bound[0]
      ma = bound[1]
      high = results[results['RELATIVE_ERROR'] < ma]
      high = high[high['RELATIVE_ERROR'] >= mi]
      low = results[results['RELATIVE_ERROR'] <= 1/mi]
      low = low[low['RELATIVE_ERROR'] > 1/ma]    
      percentages.append(len(low) + len(high))
      
  return np.array(percentages) * 100 / len(results)

def print_aggregated_metrics(results, query_frequencies):
  print(f"There are {len(results)} operators")
  print()

  percentage_bounds = [[1, 1.5], [1.5, 3], [3, 100]]
  percentages = get_percentage_bounds(results, percentage_bounds)

  for index, percentage in enumerate(percentages):
    print(f"{np.int64(percentage)}% of the operator estimates are over- or underestimated between factor {percentage_bounds[index][0]} and {percentage_bounds[index][1]}")
  print()


  ESTIMATE_COLUMN = "RUNTIME_ESTIMATE"
  MEASURED_COLUMN = "RUNTIME_CLUSTERED"

  estimates_ms = results.apply(lambda x: x[ESTIMATE_COLUMN] * query_frequencies[x['QUERY_HASH1']] / 1e6, axis=1)
  measured_ms = results.apply(lambda x: x[MEASURED_COLUMN] * query_frequencies[x['QUERY_HASH1']] / 1e6, axis=1)

  mse_milliseconds = sklearn.metrics.mean_squared_error(measured_ms, estimates_ms)

  def smape(y_true, y_pred):
      denominator = (np.abs(y_true) + np.abs(y_pred))
      diff = np.abs(y_true - y_pred) / denominator
      diff[denominator == 0] = 0.0
      return 200 * np.mean(diff)

  
  print(f"Total estimate: {'{:,}'.format(np.int64(estimates_ms.sum()))} ms")
  print(f"Total measured: {'{:,}'.format(np.int64(measured_ms.sum()))} ms")
  print(f"MSE: {'{:,}'.format(np.int64(mse_milliseconds))} ms^2")
  print(f"SMAPE: {np.int64(smape(measured_ms, estimates_ms))}%")
