
class AbstractModel:
    
    def __init__(self, query_frequencies, table_name, table_scans, correlations={}):
        self.query_frequencies = query_frequencies
        self.table_name = table_name
        self.table_scans = table_scans
        self.correlations = correlations
        
    def query_frequency(self, query_hash):
        return self.query_frequencies[query_hash]
        
    def extract_scan_columns(self):
        useful_scans = self.table_scans[self.table_scans['useful_for_pruning']]
        interesting_scan_columns = list(useful_scans['COLUMN_NAME'].unique())
        
        return interesting_scan_columns
    
    def extract_join_columns(self):
        interesting_join_probe_columns = list(self.joins[self.joins['PROBE_TABLE'] == self.table_name]['PROBE_COLUMN'].unique())
        interesting_join_build_columns = list(self.joins[self.joins['BUILD_TABLE'] == self.table_name]['BUILD_COLUMN'].unique())        
        
        return self.uniquify(interesting_join_probe_columns + interesting_join_build_columns)
    
    def extract_interesting_columns(self):        
        return self.uniquify(self.extract_scan_columns() + self.extract_join_columns())
    
    def round_up_to_next_multiple(self, number_to_round, base_for_multiple):
        quotient = number_to_round // base_for_multiple
        if number_to_round % base_for_multiple != 0:
            quotient += 1
        return quotient * base_for_multiple        

    def uniquify(self, seq):
            seen = set()
            return [x for x in seq if not (x in seen or seen.add(x))]    
    
    # return a list of possible clusterings
    def suggest_clustering(self, first_k=1):
        raise NotImplemented()