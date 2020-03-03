from abstract_benchmark import AbstractBenchmark

class TPCDSBenchmark(AbstractBenchmark):
  def name(self):
  	return "tpcds"

  def exec_path(self):
    return "/home/Alexander.Loeser/hyrise/build-release/hyriseBenchmarkTPCDS"

  def result_path(self):
    return "/home/Alexander.Loeser/hyrise/benchmark_results/tpcds"

  def time(self):
    return 45

  def scale(self):
    return 1

  def chunk_sizes(self):
    #return [25000, 100000]
    return [100000]

  def sort_orders(self):
    return {
      "nosort": {},
      "cd_education_status": {
        "customer_demographics": ["cd_education_status"]
      },
      #"ss_net_profit": {
      #  "store_sales": ["ss_net_profit"]
      #},
      #"ca_state": {
      #  "customer_address": ["ca_state"]
      #},
      "ss_2d": {
        "store_sales": ["ss_net_profit", "ss_quantity"]
      },
      "ss_2d_2": {
        "store_sales": ["ss_quantity", "ss_net_profit"]
      },
      "cd_2d": {
        "customer_demographics": ["cd_education_status", "cd_marital_status"]
      },
      "cd_2d_2": {
        "customer_demographics": ["cd_marital_status", "cd_education_status"]
      },
      #"ss_aggregate": {
      #  "store_sales": ['ss_customer_sk', 'ss_item_sk']
      #},
      #"cs_aggregate": {
      #  "catalog_sales": ["cs_bill_customer_sk", "cs_item_sk"]
      #},
    }
