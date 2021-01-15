import pandas as pd
import numpy as np
from collections import Counter


class PQPInputParser(object):
  
  """PQPInputParser - a class to parse statistics from Martin's PQP-Export-Plugin"""
  def __init__(self, benchmark_name, statistics_path):
    self.benchmark_name = benchmark_name
    self.statistics_path = statistics_path


  """Load all statistics"""
  def load_statistics(self):
    self.plan_cache = pd.read_csv(f"{self.statistics_path}/plan_cache.csv", sep='|')
    
    self.column_statistics = pd.read_csv(f"{self.statistics_path}/column_meta_data.csv", sep='|')
    self.column_statistics['DISTINCT_VALUES'] = np.int32(self.column_statistics['DISTINCT_VALUES'])

    self.table_statistics = pd.read_csv(f"{self.statistics_path}/table_meta_data.csv", sep='|')
    self.table_sizes = dict(zip(self.table_statistics.TABLE_NAME, self.table_statistics.ROW_COUNT))

    self.table_scans = pd.read_csv(f"{self.statistics_path}/table_scans.csv", sep='|')
    self.process_scans()

    self.joins = pd.read_csv(f"{self.statistics_path}/joins.csv", sep='|')
    self.joins = self.joins.dropna()
    self.process_joins()

  """Returns a mapping from query (hash) to execution count"""
  def get_query_frequencies(self):    
    return dict(zip(self.plan_cache.QUERY_HASH, self.plan_cache.EXECUTION_COUNT))

  """Returns a 2-level-dictionary: distinct_values[TABLE][COLUMN] = number_of_distinct_values"""
  def get_distinct_values_count(self):
    tables_and_columns = self.column_statistics.groupby('TABLE_NAME')
    distinct_values = { table: dict(zip(column_df.COLUMN_NAME, column_df.DISTINCT_VALUES)) for table, column_df in tables_and_columns }
    
    return distinct_values

  """Returns a dictionary: sorted_columns_during_creation[TABLE] = [column1, column2, ...]"""
  def get_sorted_columns_during_creation(self):
    globally_sorted_columns = self.column_statistics[self.column_statistics['IS_GLOBALLY_SORTED'] == 1]
    
    tables_and_columns = globally_sorted_columns.groupby('TABLE_NAME')
    globally_sorted_columns = {table: list(column_df.COLUMN_NAME) for table, column_df in tables_and_columns }
    
    return globally_sorted_columns


  """Returns a dictionary, containing the size of individual tables"""
  def get_table_sizes(self):  
    return self.table_sizes


  def get_scans(self):
    return self.table_scans

  def process_scans(self):
    # Add statistics about selectivity and speed for each operator
    scans = self.table_scans.copy()
    scans['selectivity'] = scans['OUTPUT_ROW_COUNT'] / scans['INPUT_ROW_COUNT']

    # TODO: Assumption that reading and writing a row have the same cost
    scans['time_per_row'] = scans['RUNTIME_NS'] / (scans['INPUT_ROW_COUNT'] + scans['OUTPUT_ROW_COUNT'])
    scans['time_per_input_row'] = scans['time_per_row']
    scans['time_per_output_row'] = scans['time_per_row']


    def determine_or_chains(table_scans):
        table_scans['part_of_or_chain'] = False
        
        single_table_scans = table_scans.groupby(['QUERY_HASH', 'TABLE_NAME', 'GET_TABLE_HASH'])
        
        for _, scans in single_table_scans:
            input_row_frequencies = Counter(scans.INPUT_ROW_COUNT)
            or_input_sizes = set([input_size for input_size, frequency in input_row_frequencies.items() if frequency > 1])

            df = pd.DataFrame()
            df['INPUT_ROW_COUNT'] = scans['INPUT_ROW_COUNT']
            df['OUTPUT_ROW_COUNT'] = scans['OUTPUT_ROW_COUNT']
            df['part_of_or_chain'] = scans.apply(lambda row: row['INPUT_ROW_COUNT'] in or_input_sizes, axis=1)

            for _ in range(len(scans)):
                or_input_sizes |= set(df[df['part_of_or_chain']].OUTPUT_ROW_COUNT.unique())
                df['part_of_or_chain'] = df.apply(lambda row: row['INPUT_ROW_COUNT'] in or_input_sizes, axis=1)

            or_chains = list(df[df['part_of_or_chain']].index)
            #table_scans.iloc[or_chains, table_scans.columns.get_loc('part_of_or_chain')] = True
            table_scans.loc[or_chains, 'part_of_or_chain'] = True
        
        return table_scans

    def test_determine_or_chains():
      test = pd.DataFrame()
      test['QUERY_HASH'] = pd.Series(['1']*3  + ['2']*4)
      test['TABLE_NAME'] = pd.Series(['lineitem']*3  + ['part']*4)
      test['GET_TABLE_HASH'] = pd.Series(['0x1'] + ['0x2']*2 + ['0x3']*4)
      test['COLUMN_NAME'] = pd.Series(['l_shipdate', 'l_shipdate', 'l_discount', 'p_brand', 'p_type', 'p_type', 'p_size'])
      test['INPUT_ROW_COUNT'] = pd.Series( [6001215, 6001215, 200000, 200000, 199000, 199000, 50000])
      test['OUTPUT_ROW_COUNT'] = pd.Series([ 400000,  300000, 200000, 199000,      0,  50000, 20000])
      test_result = determine_or_chains(test)
      assert len(test_result) == 7, "should not filter out any rows"    
      assert len(test_result[test_result['part_of_or_chain']]) == 3, "expected 3 scans, got\n" + str(test_result)
      assert list(test_result['part_of_or_chain']) == [False]*4 + [True]*3
      print("Test OK")

    # Hyrise does not use scans that are part of an OR-chain for pruning
    scans = determine_or_chains(scans)


    # Like scans are not useful if they start with %
    # TODO what if they dont start with % and contain more than one % ? -> up to first % prunable, but is it used?
    def benefits_from_sorting(row):    
        description = row['DESCRIPTION']
        if "ColumnLike" in description:
            words = description.split('LIKE')
            assert len(words) == 2, f"expected exactly one occurence of LIKE, but got {description}"
            like_criteria = words[1]
            assert "%" in like_criteria or "_" in like_criteria, f"LIKE operators should have an % or _, but found none in {like_criteria}"
            first_char = like_criteria[2]
            assert first_char != ' ' and first_char != "'", "Like check considers the wrong token"
            return first_char != '%' and first_char != '_'
        elif "ExpressionEvaluator" in description and " IN " in description:
            return False
        else:
            return True

    scans['benefits_from_sorting'] = scans.apply(benefits_from_sorting, axis=1)
    # TODO: valid atm, but feels a bit hacky to assume not benefitting from sorted segments -> not benefitting from pruning
    scans['useful_for_pruning'] = scans.apply(lambda row: not row['part_of_or_chain'] and row['benefits_from_sorting'] , axis=1)

    # Add/rename fields to match the cost model schema
    scans['OPERATOR_IMPLEMENTATION'] = scans.apply(lambda x: x['DESCRIPTION'].split("Impl: ")[1].split()[0], axis=1)
    scans.rename(inplace=True, columns={
        'selectivity': 'SELECTIVITY_LEFT',
        'INPUT_ROW_COUNT': 'INPUT_ROWS',
        'OUTPUT_ROW_COUNT': 'OUTPUT_ROWS',
        'PREDICATE_CONDITION': 'PREDICATE',
        'INPUT_CHUNK_COUNT': 'INPUT_CHUNKS',
    })

    self.table_scans = scans.copy()


  def get_joins(self):
    return self.joins

  def process_joins(self):
    def line_looks_suspicious(row):
        right_table_name = row['RIGHT_TABLE_NAME']    
        if pd.isnull(right_table_name):
            pass
        elif row['RIGHT_TABLE_ROW_COUNT'] > self.table_sizes[row['RIGHT_TABLE_NAME']]:
            return True

        left_table_name = row['LEFT_TABLE_NAME']
        if pd.isnull(left_table_name):
            pass
        elif row['LEFT_TABLE_ROW_COUNT'] > self.table_sizes[row['LEFT_TABLE_NAME']]:
            return True

        return False
    
    def validate_joins(joins):
        is_suspicious = joins.apply(line_looks_suspicious, axis=1)
        suspicious_joins = joins[is_suspicious]
        assert len(suspicious_joins) < 4, f"there are {len(suspicious_joins)} suspicious joins:\n{suspicious_joins[['JOIN_MODE', 'LEFT_TABLE_NAME', 'LEFT_COLUMN_NAME', 'LEFT_TABLE_ROW_COUNT', 'RIGHT_TABLE_NAME', 'RIGHT_COLUMN_NAME', 'RIGHT_TABLE_ROW_COUNT', 'OUTPUT_ROW_COUNT']]}"
    
    # TODO: maybe move this out to some later part
    joins = self.joins.copy()
    joins['PROBE_TABLE'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_TABLE_NAME"] if not x['PROBE_SIDE'] == "NULL" else "NULL", axis=1)
    joins['PROBE_COLUMN'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_COLUMN_NAME"] if not x['PROBE_SIDE'] == "NULL" else "NULL", axis=1)
    joins['PROBE_TABLE_ROW_COUNT'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_TABLE_ROW_COUNT"]if not x['PROBE_SIDE'] == "NULL" else "NULL" , axis=1)
    joins['BUILD_TABLE'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_TABLE_NAME"] if not x['BUILD_SIDE'] == "NULL" else "NULL", axis=1)
    joins['BUILD_COLUMN'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_COLUMN_NAME"] if not x['BUILD_SIDE'] == "NULL" else "NULL", axis=1)
    joins['BUILD_TABLE_ROW_COUNT'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_TABLE_ROW_COUNT"] if not x['BUILD_SIDE'] == "NULL" else "NULL", axis=1)
    validate_joins(joins)

    joins['OPERATOR_IMPLEMENTATION'] = joins.apply(lambda x: x['DESCRIPTION'].split()[0], axis=1)
    joins['BUILD_COLUMN_TYPE'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_COLUMN_TYPE"], axis=1)
    joins['BUILD_INPUT_CHUNKS'] = joins.apply(lambda x: x[x['BUILD_SIDE'] + "_TABLE_CHUNK_COUNT"], axis=1)
    joins['PROBE_COLUMN_TYPE'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_COLUMN_TYPE"], axis=1)
    joins['PROBE_INPUT_CHUNKS'] = joins.apply(lambda x: x[x['PROBE_SIDE'] + "_TABLE_CHUNK_COUNT"], axis=1)

    self.joins = joins.copy()

  """Extract interesting table names. Currently fixed to columns which are scanned or used in joins"""
  def get_table_names(self):
    scan_tables = set(self.table_scans['TABLE_NAME'].unique())
    left_join_tables = set(self.joins['LEFT_TABLE_NAME'].unique())
    right_join_tables = set(self.joins['RIGHT_TABLE_NAME'].unique())    

    return scan_tables.union(left_join_tables.union(right_join_tables))
