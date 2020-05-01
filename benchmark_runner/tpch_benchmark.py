from abstract_benchmark import AbstractBenchmark

class TPCHBenchmark(AbstractBenchmark):
  def name(self):
  	return "tpch"

  def exec_path(self):
    return "/home/Alexander.Loeser/hyrise/build-release/hyrisePlayground"

  def result_path(self):
    return "/home/Alexander.Loeser/hyrise/benchmark_results/tpch_disjoint_clusters_mvcc_removechunks2_sf_1"

  def max_runs(self):
    return -1

  def time(self):
    return 60

  def scale(self):
    return 1
  
  def chunk_sizes(self):
    return [25000]

  def sort_orders(self):
    # SF 1 test disjoin_clusters
    return  {
      'l_shipdate-10_o_orderdate-10': {
        'lineitem': [['l_shipdate', 10]],
        'orders': [['o_orderdate', 10]]
      },
      'l_shipdate-25_o_orderdate-20': {
        'lineitem': [['l_shipdate', 25]],
        'orders': [['o_orderdate', 20]]
      },
      'l_shipdate-80_o_orderdate-40': {
        'lineitem': [['l_shipdate', 80]],
        'orders': [['o_orderdate', 40]]
      },
      'l_shipdate-40_l_discount-2_o_orderdate-40': {
        'lineitem': [['l_shipdate', 40,], ['l_discount', 2]],
        'orders': [['o_orderdate', 40]]
      },
      'l_shipdate-80_l_discount-3_o_orderdate-40': {
        'lineitem': [['l_shipdate', 80,], ['l_discount', 3]],
        'orders': [['o_orderdate', 40]]
      },
    }





        # SF 1
    return  {
      #'l_shipdate-27_l_discount-9_o_orderdate-60': {
      #  'lineitem': [['l_shipdate', 27], ['l_discount', 9]],
      #  'orders': [['o_orderdate', 60]]
      #},

      #'l_shipdate-27_l_discount-9_l_shipdate-1_o_orderdate-60': {
      #  'lineitem': [['l_shipdate', 27], ['l_discount', 9], ['l_shipdate', 1]],
      #  'orders': [['o_orderdate', 60]]
      #},

      #'l_discount-9_l_shipdate-27_o_orderdate-60': {
      #  'lineitem': [['l_discount', 9], ['l_shipdate', 27]],
      #  'orders': [['o_orderdate', 60]]
      #},
      #'l_discount-9_l_shipdate-27_l_discount-1_o_orderdate-60': {
      #  'lineitem': [['l_discount', 9], ['l_shipdate', 27], ['l_discount', 1]],
      #  'orders': [['o_orderdate', 60]]
      #},
     #'l_shipdate-120_l_discount-2_o_orderdate-60': {
     #   'lineitem': [['l_shipdate', 120], ['l_discount', 2]],
     #   'orders': [['o_orderdate', 60]]
     #},

      #'l_shipdate-120_l_discount-2_l_shipdate-1_o_orderdate-60': {
      #  'lineitem': [['l_shipdate', 120], ['l_discount', 2], ['l_shipdate', 1]],
      #  'orders': [['o_orderdate', 60]]
      #},

      #'l_discount-2_l_shipdate-120_o_orderdate-60': {
      #  'lineitem': [['l_discount', 2], ['l_shipdate', 120]],
      #  'orders': [['o_orderdate', 60]]
      #},
      #'l_discount-2_l_shipdate-120_l_discount-1_o_orderdate-60': {
      #  'lineitem': [['l_discount', 2], ['l_shipdate', 120], ['l_discount', 1]],
      #  'orders': [['o_orderdate', 60]]
      #},

      #'nosort': {},
      'default': {'lineitem': [['l_shipdate', 241]],
  'orders': [['o_orderdate', 60]]},
 'l_shipdate-241_o_orderdate-16_o_orderstatus-4_o_orderdate-1': {'lineitem': [['l_shipdate',
    241]],
  'orders': [('o_orderdate', 16), ('o_orderstatus', 4), ('o_orderdate', 1)]},
 'l_shipdate-241_o_orderstatus-60_o_orderdate-1': {'lineitem': [['l_shipdate',
    241]],
  'orders': [('o_orderstatus', 60), ('o_orderdate', 1)]},
 'l_shipdate-241_o_orderdate-16_o_orderstatus-4': {'lineitem': [['l_shipdate',
    241]],
  'orders': [('o_orderdate', 16), ('o_orderstatus', 4)]},
 'l_shipdate-241_o_orderdate-60_p_name-5_p_size-2_p_type-1': {'lineitem': [['l_shipdate',
    241]],
  'orders': [['o_orderdate', 60]],
  'part': [('p_name', 5), ('p_size', 2), ('p_type', 1)]},
 'l_shipdate-241_o_orderdate-60_p_brand-3_p_size-4_p_type-1': {'lineitem': [['l_shipdate',
    241]],
  'orders': [['o_orderdate', 60]],
  'part': [('p_brand', 3), ('p_size', 4), ('p_type', 1)]},
 'l_shipdate-241_o_orderdate-60_p_container-3_p_size-4_p_type-1': {'lineitem': [['l_shipdate',
    241]],
  'orders': [['o_orderdate', 60]],
  'part': [('p_container', 3), ('p_size', 4), ('p_type', 1)]},
 'l_discount-9_l_receiptdate-27_l_shipdate-1_o_orderdate-60': {'lineitem': [('l_discount',
    9),
   ('l_receiptdate', 27),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 60]]},
 'l_quantity-12_l_receiptdate-21_l_shipdate-1_o_orderdate-60': {'lineitem': [('l_quantity',
    12),
   ('l_receiptdate', 21),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 60]]},
 'l_receiptdate-16_l_shipdate-16_o_orderdate-60': {'lineitem': [('l_receiptdate',
    16),
   ('l_shipdate', 16)],
  'orders': [['o_orderdate', 60]]},
 'l_shipdate-241_o_orderdate-60_c_mktsegment-6': {'lineitem': [['l_shipdate',
    241]],
  'orders': [['o_orderdate', 60]],
  'customer': [('c_mktsegment', 6)]}
    }

    # SF 10
    return {'default': {'lineitem': [['l_shipdate', 2400]],
  'orders': [['o_orderdate', 600]]},
 'l_shipdate-2400_o_orderstatus-600_o_orderdate-1': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [('o_orderstatus', 600), ('o_orderdate', 1)]},
 'l_shipdate-2400_o_orderdate-49_o_orderstatus-13_o_orderdate-1': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [('o_orderdate', 49), ('o_orderstatus', 13), ('o_orderdate', 1)]},
 'l_shipdate-2400_o_orderdate-600_o_orderstatus-1': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [('o_orderdate', 600), ('o_orderstatus', 1)]},
 'l_shipdate-2400_o_orderdate-600_p_name-16_p_size-6_p_type-1': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [['o_orderdate', 600]],
  'part': [('p_name', 16), ('p_size', 6), ('p_type', 1)]},
 'l_shipdate-2400_o_orderdate-600_p_brand-9_p_size-10_p_type-1': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [['o_orderdate', 600]],
  'part': [('p_brand', 9), ('p_size', 10), ('p_type', 1)]},
 'l_shipdate-2400_o_orderdate-600_p_container-9_p_size-10_p_type-1': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [['o_orderdate', 600]],
  'part': [('p_container', 9), ('p_size', 10), ('p_type', 1)]},
 'l_receiptdate-2400_l_shipdate-1_o_orderdate-600': {'lineitem': [('l_receiptdate',
    2400),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 600]]},
 'l_discount-29_l_receiptdate-85_l_shipdate-1_o_orderdate-600': {'lineitem': [('l_discount',
    29),
   ('l_receiptdate', 85),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 600]]},
 'l_receiptdate-85_l_shipmode-29_l_shipdate-1_o_orderdate-600': {'lineitem': [('l_receiptdate',
    85),
   ('l_shipmode', 29),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 600]]},
 'l_shipdate-2400_o_orderdate-600_c_mktsegment-60': {'lineitem': [['l_shipdate',
    2400]],
  'orders': [['o_orderdate', 600]],
  'customer': [('c_mktsegment', 60)]}
  }


