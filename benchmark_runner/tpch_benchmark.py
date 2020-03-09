from abstract_benchmark import AbstractBenchmark

class TPCHBenchmark(AbstractBenchmark):
  def name(self):
  	return "tpch"

  def exec_path(self):
    return "/home/Alexander.Loeser/hyrise/build-release/hyriseBenchmarkTPCH"

  def result_path(self):
    return "/home/Alexander.Loeser/hyrise/benchmark_results/tpch_random_access_tmp"

  def time(self):
    return 60

  def scale(self):
    return 1
  
  def chunk_sizes(self):
    return [25000]

  def sort_orders(self):
    return {
      #"nosort": {},
      #"default": {
      #  "lineitem": [["l_shipdate", 100]],
      #  "orders": [["o_orderdate", 50]]
      #},
      #"highest_1d_gain": {
      #  "customer": ["c_mktsegment"],
      #  "lineitem": ["l_receiptdate"],
      #  "orders": ["o_orderdate"],
      #  "part": ["p_brand"],
      #  "partsupp": ["ps_suppkey"]
      #},
      #"lineitem_2d_gain": {
      #  "lineitem": ["l_shipdate", "l_receiptdate"],
      #  "orders": ["o_orderdate"]
      #},
      #"q11_test": {
      #  "lineitem": ["l_shipdate", "l_suppkey"],
      #  "orders": ["o_orderdate"],
      #  "partsupp": ["ps_suppkey"]
      #},
      #"q11_join": {
      #  "lineitem": ["l_shipdate"],
      #  "orders": ["o_orderdate"],
      #  "partsupp": ["ps_suppkey"]
      #},
      #"q11_test_2": {
      #  "lineitem": ["l_suppkey", "l_shipdate"],
      #  "orders": ["o_orderdate"],
      #  "partsupp": ["ps_suppkey"]
      #},
      #"l_aggregate": {
      #  "lineitem": ["l_shipdate", "l_orderkey"],
      #  "orders": ["o_orderdate"]
      #},
      "q6_discount": {
        "clustering_order": {
          "lineitem": [
            ["l_shipdate", 75]
          ],
          "orders": [
            ["o_orderdate", 50]
          ]
        },
        "chunk_order": {
          "lineitem": "l_discount"
        }
      },
      "q6_2d_discount_discount": {
        "clustering_order": {
          "lineitem": [
            ["l_shipdate", 75],
            ["l_discount", 2]
          ],
          "orders": [
            ["o_orderdate", 50]
          ]
        }
      },
      "q6_2d_discount_shipdate": {
        "clustering_order": {
          "lineitem": [
            ["l_shipdate", 75],
            ["l_discount", 2]
          ],
          "orders": [
            ["o_orderdate", 50]
          ]
        },
        "chunk_order": {
          "lineitem": "l_shipdate"
        }
      },
      #"q6_quantity": {
      #  "lineitem": ["l_shipdate", "l_quantity"],
      #  "orders": ["o_orderdate"]
      #},
      #"q6_discount_2": {
      #  "lineitem": ["l_discount", "l_shipdate"],
      #  "orders": ["o_orderdate"]
      #},
      #"q6_quantity_2": {
      #  "lineitem": ["l_quantity", "l_shipdate"],
      #  "orders": ["o_orderdate"]
      #},
      #"l_suppkey_2d": {
      #  "lineitem": ["l_shipdate", "l_suppkey"],
      #  "orders": ["o_orderdate"]
      #},
      #"l_suppkey_2d_2": {
      #  "lineitem": ["l_suppkey", "l_shipdate"],
      #  "orders": ["o_orderdate"]
      #},
      #"l_suppkey_commit": {
      #  "lineitem": ["l_commitdate", "l_suppkey"],
      #  "orders": ["o_orderdate"]
      #},
      #"l_suppkey_commit_2": {
      #  "lineitem": ["l_suppkey", "l_commitdate"],
      #  "orders": ["o_orderdate"]
      #},
    }
