from abstract_benchmark import AbstractBenchmark

class TPCHBenchmark(AbstractBenchmark):
  def name(self):
  	return "tpch"

  def exec_path(self):
    return "/home/Alexander.Loeser/hyrise/build-release/hyrisePlayground"

  def result_path(self):
    return "/home/Alexander.Loeser/hyrise/benchmark_results/tpch/cluster_size_experiments"

  def max_runs(self):
    return -1

  def time(self):
    return 60

  def scale(self):
    return 1
  
  def chunk_sizes(self):
    return [65535]

  def sort_orders(self):
##
    return {
      'ex10-l_orderkey-2_l_shipdate-46_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 2], ['l_shipdate', 46], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },
    }


    return {
      'ex9-l_suppkey-5_l_orderkey-5_l_partkey-5_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_suppkey', 5], ['l_orderkey', 5], ['l_partkey', 5], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex9-l_suppkey-5_l_orderkey-5_l_partkey-5_l_shipdate-1_o_orderdate-2': {
        'lineitem': [['l_suppkey', 5], ['l_orderkey', 5], ['l_partkey', 5], ['l_shipdate', 1]],
        'orders': [['o_orderdate', 2]]
      },


      'ex9-l_shipdate-12_l_orderkey-3_l_partkey-3_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_shipdate', 12], ['l_orderkey', 3], ['l_partkey', 3], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },

      # this one is hand written
      'ex9-l_shipdate-36_l_orderkey-3_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_shipdate', 36], ['l_orderkey', 3], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },

      'ex9-l_receiptdate-12_l_orderkey-3_l_partkey-3_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_receiptdate', 12], ['l_orderkey', 3], ['l_partkey', 3], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },

      'ex9-l_shipdate-4_l_suppkey-3_l_orderkey-3_l_partkey-3_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_shipdate', 4], ['l_suppkey', 3], ['l_orderkey', 3], ['l_partkey', 3], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },


      # hand written 3x
      'ex10-l_orderkey-5_l_shipdate-20_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 5], ['l_shipdate', 20], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex10-l_orderkey-10_l_shipdate-10_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 10], ['l_shipdate', 10], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex10-l_orderkey-20_l_shipdate-5_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 20], ['l_shipdate', 5], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      }
    }



    return {
      'ex8-l_orderkey-3_l_shipdate-34_l_partkey-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 3], ['l_shipdate', 34], ['l_partkey', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex8-l_partkey-3_l_shipdate-34_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_partkey', 3], ['l_shipdate', 34], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },

      'ex8-l_orderkey-10_l_partkey-10_l_orderkey-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 10], ['l_partkey', 10], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex8-l_orderkey-10_l_partkey-10_l_shipdate-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 10], ['l_partkey', 10], ['l_shipdate', 1]],
        'orders': [['o_orderdate', 2]]
      },
    }

    return {
      'ex-6_l_orderkey-3_l_shipdate-10-l_discount-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 3], ['l_shipdate', 10], ['l_discount', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex-6_l_orderkey-6_l_shipdate-10-l_discount-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 6], ['l_shipdate', 10], ['l_discount', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex-6_l_orderkey-9_l_shipdate-10-l_discount-1_o_orderdate-2': {
        'lineitem': [['l_orderkey', 9], ['l_shipdate', 10], ['l_discount', 1]],
        'orders': [['o_orderdate', 2]]
      },
      'ex-7_l_shipdate-10-l_orderkey-3_o_orderdate-2': {
        'lineitem': [['l_shipdate', 10], ['l_orderkey', 3]],
        'orders': [['o_orderdate', 2]]
      },
      'ex-7_l_shipdate-10-l_orderkey-6_o_orderdate-2': {
        'lineitem': [['l_shipdate', 10], ['l_orderkey', 6]],
        'orders': [['o_orderdate', 2]]
      },
      'ex-7_l_shipdate-10-l_orderkey-9_o_orderdate-2': {
        'lineitem': [['l_shipdate', 10], ['l_orderkey', 9]],
        'orders': [['o_orderdate', 2]]
      },
    }

  # SF 10 RUNS 10
    return {
      #'nosort': {},
      'default': {
        'lineitem': [['l_shipdate', 2]],
        'orders': [['o_orderdate', 2]]
      },

      'l_orderkey-36_l_suppkey-26_l_partkey-1_o_orderdate-2': {
        'lineitem': [('l_orderkey',36),('l_suppkey', 26), ('l_partkey', 1)],
        'orders': [['o_orderdate', 2]]
      },

      'l_orderkey-33_l_partkey-26_l_shipdate-1_o_orderdate-2': {
        'lineitem': [('l_orderkey',33),('l_partkey', 26), ('l_shipdate', 1)],
        'orders': [['o_orderdate', 2]]
      },

      'l_orderkey-33_l_partkey-26_l_orderkey-1_o_orderdate-2': {
        'lineitem': [('l_orderkey',33),('l_partkey', 26), ('l_orderkey', 1)],
        'orders': [['o_orderdate', 2]]
      },

      'l_partkey-26_l_orderkey-33_o_orderdate-2': {
        'lineitem': [('l_partkey', 26), ('l_orderkey',33)],
        'orders': [['o_orderdate', 2]]
      },
      
      'l_orderkey-15_l_partkey-15_l_shipdate-1_o_orderdate-2': {
        'lineitem': [('l_orderkey',15),('l_partkey', 15), ('l_shipdate', 1)],
        'orders': [['o_orderdate', 2]]
      },



      'l_shipdate-30-l_discount-2-l_orderkey-10_o_orderdate-2': {
        'lineitem': [('l_shipdate', 30), ('l_discount', 2), ('l_orderkey', 10)],
        'orders': [['o_orderdate', 2]]
      },

      'l_orderkey-13_l_partkey-12_l_shipdate-7_l_orderkey-1_o_orderdate-2': {
        'lineitem': [('l_orderkey', 13),('l_partkey', 12),('l_shipdate', 7),('l_orderkey', 1)],
        'orders': [['o_orderdate', 2]]
      },
      'l_shipdate-7-l_partkey-12_l_orderkey-13_o_orderdate-2': {
        'lineitem': [('l_shipdate', 7),('l_partkey', 12),('l_orderkey', 13)],
        'orders': [['o_orderdate', 2]]
      },


      'l_partkey-15_l_orderkey-15_o_orderdate-2': {
        'lineitem': [('l_partkey', 15), ('l_orderkey',15)],
        'orders': [['o_orderdate', 2]]
      },

      'l_shipdate-2_o_custkey-21_o_orderdate-12_o_orderkey-1': {
        'lineitem': [['l_shipdate', 2]],
        'orders': [('o_custkey', 21), ('o_orderdate', 12), ('o_orderkey', 1)]
      },

      'l_shipdate-2_o_custkey-14_o_orderkey-17': {
        'lineitem': [['l_shipdate', 2]],
        'orders': [('o_custkey', 14), ('o_orderkey', 17)]
      },

      'l_shipdate-2_o_custkey-14_o_orderdate-1_o_orderkey-17': {
        'lineitem': [['l_shipdate', 2]],
        'orders': [('o_custkey', 14), ('o_orderkey', 17), ('o_orderdate', 1)]
      },
    }



    return {
      'ex2-merge-l_shipdate-30_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 30], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },

      'ex2-merge-l_discount-3_l_shipdate-30_l_discount-1_o_orderdate-23': {
        'lineitem': [['l_discount', 3], ['l_shipdate', 30], ['l_discount', 1]],
        'orders': [['o_orderdate', 23]]
      },
    }

    return  {

      'ex5-l_orderkey-3_l_shipdate-8_o_orderdate-23': {
        'lineitem': [['l_orderkey', 3], ['l_shipdate', 8]],
        'orders': [['o_orderdate', 23]]
      },
      'ex5-l_orderkey-6_l_shipdate-8_o_orderdate-23': {
        'lineitem': [['l_orderkey', 6], ['l_shipdate', 8]],
        'orders': [['o_orderdate', 23]]
      },
      'ex5-l_l_orderkey-12_shipdate-8_o_orderdate-23': {
        'lineitem': [['l_orderkey', 12], ['l_shipdate', 8]],
        'orders': [['o_orderdate', 23]]
      },
    }

  # more experiments
    return {
    # A, B, C == B, A, C?
      'ex1-l_shipdate-10_l_orderkey-10_l_discount-1_o_orderdate-23': {
        'lineitem': [['l_shipdate', 10], ['l_orderkey', 10], ['l_discount', 1]],
        'orders': [['o_orderdate', 23]]
      },
      'ex1-l_orderkey-10_l_shipdate-10_l_discount-1_o_orderdate-23': {
        'lineitem': [['l_orderkey', 10], ['l_shipdate', 10], ['l_discount', 1]],
        'orders': [['o_orderdate', 23]]
      },

    # A, B, A1 == B, A?
      'ex2-l_shipdate-30_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 30], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },

      'ex2-l_discount-3_l_shipdate-30_l_discount-1_o_orderdate-23': {
        'lineitem': [['l_discount', 3], ['l_shipdate', 30], ['l_discount', 1]],
        'orders': [['o_orderdate', 23]]
      },

    # influence of pruning column cluster size (and maybe sort-influence?)
      'ex3-l_shipdate-10_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 10], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },

      'ex3-l_shipdate-20_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 20], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },

      'ex3-l_shipdate-30_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 30], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },

      'ex3-l_shipdate-40_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 40], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },

      'ex3-l_shipdate-40_l_discount-2_o_orderdate-23': {
        'lineitem': [['l_shipdate', 40], ['l_discount', 2]],
        'orders': [['o_orderdate', 23]]
      },

      # influence of join column cluster size
      'ex4-l_shipdate-8_l_orderkey-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 8], ['l_orderkey', 3]],
        'orders': [['o_orderdate', 23]]
      },
      'ex4-l_shipdate-8_l_orderkey-6_o_orderdate-23': {
        'lineitem': [['l_shipdate', 8], ['l_orderkey', 6]],
        'orders': [['o_orderdate', 23]]
      },
      'ex4-l_shipdate-8_l_orderkey-12_o_orderdate-23': {
        'lineitem': [['l_shipdate', 8], ['l_orderkey', 12]],
        'orders': [['o_orderdate', 23]]
      },
    }


    return {
     'l_shipdate-15_l_discount-2_l_orderkey-4_o_orderdate-23': {
        'lineitem': [['l_shipdate', 15], ['l_discount', 2], ['l_orderkey', 4]],
        'orders': [['o_orderdate', 23]]
      },


      'l_shipdate-15_l_discount-2_l_orderkey-4_o_orderdate-2': {
        'lineitem': [['l_shipdate', 15], ['l_discount', 2], ['l_orderkey', 4]],
        'orders': [['o_orderdate', 2]]
      },     
      'l_shipdate-20_l_discount-2_l_orderkey-4_o_orderdate-2': {
        'lineitem': [['l_shipdate', 20], ['l_discount', 2], ['l_orderkey', 4]],
        'orders': [['o_orderdate', 2]]
      },
      'l_shipdate-15_l_discount-3_l_orderkey-4_o_orderdate-2': {
        'lineitem': [['l_shipdate', 15], ['l_discount', 3], ['l_orderkey', 4]],
        'orders': [['o_orderdate', 2]]
      },

      'l_shipdate-15_l_orderkey-4_l_discount-2_o_orderdate-23': {
        'lineitem': [['l_shipdate', 15], ['l_orderkey', 4], ['l_discount', 2]],
        'orders': [['o_orderdate', 23]]
      },
      'l_shipdate-20_l_orderkey-4_l_discount-2_o_orderdate-2': {
        'lineitem': [['l_shipdate', 20], ['l_orderkey', 4], ['l_discount', 2]],
        'orders': [['o_orderdate', 2]]
      },
      'l_shipdate-15_l_orderkey-4_l_discount-3_o_orderdate-2': {
        'lineitem': [['l_shipdate', 15], ['l_orderkey', 4], ['l_discount', 3]],
        'orders': [['o_orderdate', 2]]
      },
    }


   # SF 1 experiments regarding cluster size
    return {
      'l_shipdate-50_l_discount-5_o_orderdate-1': {
        'lineitem': [['l_shipdate', 50], ['l_discount', 5]],
        'orders': [['o_orderdate', 1]]
      },

      'l_shipdate-50_l_discount-2_o_orderdate-1': {
        'lineitem': [['l_shipdate', 50], ['l_discount', 2]],
        'orders': [['o_orderdate', 1]]
      },

      'l_shipdate-20_l_discount-2_o_orderdate-1': {
        'lineitem': [['l_shipdate', 20], ['l_discount', 2]],
        'orders': [['o_orderdate', 1]]
      },
      'l_shipdate-10_l_discount-2_o_orderdate-1': {
        'lineitem': [['l_shipdate', 10], ['l_discount', 2]],
        'orders': [['o_orderdate', 1]]
      },
      'l_shipdate-10_l_discount-2_o_orderdate-23': {
        'lineitem': [['l_shipdate', 10], ['l_discount', 2]],
        'orders': [['o_orderdate', 23]]
      },
      'l_shipdate-50_l_discount-2_o_orderdate-23': {
        'lineitem': [['l_shipdate', 50], ['l_discount', 2]],
        'orders': [['o_orderdate', 23]]
      },


      'l_orderkey-10_l_shipdate-9_l_orderkey-1_o_orderdate-23': {
        'lineitem': [['l_orderkey', 10], ['l_shipdate', 9], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 23]]
      },

      'l_orderkey-20_l_shipdate-15_l_orderkey-1_o_orderdate-23': {
        'lineitem': [['l_orderkey', 20], ['l_shipdate', 15], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 23]]
      },

      'l_orderkey-30_l_shipdate-4_l_orderkey-1_o_orderdate-23': {
        'lineitem': [['l_orderkey', 30], ['l_shipdate', 4], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 23]]
      },

      'l_orderkey-5_l_shipdate-20_l_orderkey-1_o_orderdate-23': {
        'lineitem': [['l_orderkey', 5], ['l_shipdate', 20], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 23]]
      },

      'l_orderkey-10_l_shipdate-9_l_orderkey-1_o_orderdate-1': {
        'lineitem': [['l_orderkey', 10], ['l_shipdate', 9], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 1]]
      },

      'l_orderkey-20_l_shipdate-15_l_orderkey-1_o_orderdate-1': {
        'lineitem': [['l_orderkey', 20], ['l_shipdate', 15], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 1]]
      },

      'l_orderkey-30_l_shipdate-4_l_orderkey-1_o_orderdate-1': {
        'lineitem': [['l_orderkey', 30], ['l_shipdate', 4], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 1]]
      },

      'l_orderkey-5_l_shipdate-20_l_orderkey-1_o_orderdate-1': {
        'lineitem': [['l_orderkey', 5], ['l_shipdate', 20], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 1]]
      },
   }

  # experiments regarding amount/order of clusters
    return {
      'l_shipdate-30_l_discount-3_o_orderdate-23': {
        'lineitem': [['l_shipdate', 30], ['l_discount', 3]],
        'orders': [['o_orderdate', 23]]
      },
      'l_discount-3_l_shipdate-30_l_discount-1_o_orderdate-23': {
        'lineitem': [['l_discount', 3], ['l_shipdate', 30], ['l_discount', 1]],
        'orders': [['o_orderdate', 23]]
      },
      'l_orderkey-10_l_shipdate-9_o_orderdate-23': {
        'lineitem': [['l_orderkey', 10], ['l_shipdate', 9]],
        'orders': [['o_orderdate', 23]]
      },
      'l_shipdate-9_l_orderkey-10_l_shipdate-1_o_orderdate-23': {
        'lineitem': [['l_shipdate', 9], ['l_orderkey', 10], ['l_shipdate', 1]],
        'orders': [['o_orderdate', 23]]
      },
      'l_shipdate-9_l_orderkey-10_o_orderdate-23': {
        'lineitem': [['l_shipdate', 9], ['l_orderkey', 10]],
        'orders': [['o_orderdate', 23]]
      },
      'l_orderkey-10_l_shipdate-9_l_orderkey-1_o_orderdate-23': {
        'lineitem': [['l_orderkey', 10], ['l_shipdate', 9], ['l_orderkey', 1]],
        'orders': [['o_orderdate', 23]]
      },
    }


  # SF 1, Runs 10, joins with frequency, 3d
    return {'default': {'lineitem': [['l_shipdate', 92]],
  'orders': [['o_orderdate', 23]]},
 'l_shipdate-92_o_custkey-4_o_orderdate-3_o_orderkey-4': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_custkey', 4), ('o_orderdate', 3), ('o_orderkey', 4)]},
 'l_shipdate-92_o_custkey-4_o_orderdate-3_o_orderkey-4_o_orderdate-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_custkey', 4),
   ('o_orderdate', 3),
   ('o_orderkey', 4),
   ('o_orderdate', 1)]},
 'l_shipdate-92_o_custkey-5_o_orderkey-6_o_orderstatus-1_o_orderdate-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_custkey', 5),
   ('o_orderkey', 6),
   ('o_orderstatus', 1),
   ('o_orderdate', 1)]},
 'l_shipdate-92_o_orderdate-23_p_name-2_p_partkey-2_p_type-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [['o_orderdate', 23]],
  'part': [('p_name', 2), ('p_partkey', 2), ('p_type', 1)]},
 'l_shipdate-92_o_orderdate-23_p_name-3_p_partkey-3_p_size-1_p_type-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [['o_orderdate', 23]],
  'part': [('p_name', 3), ('p_partkey', 3), ('p_size', 1), ('p_type', 1)]},
 'l_orderkey-6_l_partkey-6_l_shipdate-4_l_orderkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    6),
   ('l_partkey', 6),
   ('l_shipdate', 4),
   ('l_orderkey', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_orderkey-6_l_partkey-6_l_receiptdate-4_l_orderkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    6),
   ('l_partkey', 6),
   ('l_receiptdate', 4),
   ('l_orderkey', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_orderkey-6_l_partkey-5_l_suppkey-4_l_orderkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    6),
   ('l_partkey', 5),
   ('l_suppkey', 4),
   ('l_orderkey', 1)],
  'orders': [['o_orderdate', 23]]}}


    return  {

     'multi': {
      'lineitem': [('l_orderkey', 13), ('l_shipdate', 8), ('l_partkey', 1)],
      'orders': [('o_orderdate', 4), ('o_orderkey', 7), ('o_orderstatus', 1)]


      }


    }
    return {
     'l_shipdate-104_o_orderdate-23': {'lineitem': [('l_shipdate',
    104)],
  'orders': [['o_orderdate', 23]]}
    }


    return {
'l_orderkey-11_l_partkey-9_l_shipdate-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    11),
   ('l_partkey', 9),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_orderkey-13_l_shipdate-8_l_partkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    13),
   ('l_shipdate', 8),
   ('l_partkey', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_partkey-13_l_shipdate-8_l_orderkey-1_o_orderdate-23': {'lineitem': [('l_partkey',
    13),
   ('l_shipdate', 8),
   ('l_orderkey', 1)],
  'orders': [['o_orderdate', 23]]}

   }


    return {
'l_shipdate-92_o_custkey-5_o_orderkey-6': {'lineitem': [['l_shipdate', 92]],
  'orders': [('o_custkey', 5), ('o_orderkey', 6)]},
 'l_shipdate-92_o_custkey-5_o_orderkey-6_o_orderdate-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_custkey', 5), ('o_orderkey', 6), ('o_orderdate', 1)]},
 'l_shipdate-92_o_orderdate-4_o_orderkey-7_o_custkey-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_orderdate', 4), ('o_orderkey', 7), ('o_custkey', 1)]},

    
  'l_orderkey-12_l_suppkey-8_l_partkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    12),
   ('l_suppkey', 8),
   ('l_partkey', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_orderkey-11_l_partkey-9_l_suppkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    11),
   ('l_partkey', 9),
   ('l_suppkey', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_partkey-12_l_suppkey-9_l_orderkey-1_o_orderdate-23': {'lineitem': [('l_partkey',
    12),
   ('l_suppkey', 9),
   ('l_orderkey', 1)],
  'orders': [['o_orderdate', 23]]}

    }

    return {
      'l_shipdate-46_l_discount-2_o_orderdate-23': {
        'lineitem': [['l_shipdate', 46], ['l_discount', 2]],
        'orders'  : [['o_orderdate', 23]]
      }

    }

    return {
 'nosort': {},
 'default': {'lineitem': [['l_shipdate', 92]],
  'orders': [['o_orderdate', 23]]},
 'l_shipdate-92_o_orderdate-4_o_orderkey-7_o_orderstatus-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_orderdate', 4), ('o_orderkey', 7), ('o_orderstatus', 1)]},
 'l_shipdate-92_o_orderkey-13_o_orderstatus-2': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_orderkey', 13), ('o_orderstatus', 2)]},
 'l_shipdate-92_o_orderkey-23_o_orderstatus-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [('o_orderkey', 23), ('o_orderstatus', 1)]},
 'l_shipdate-92_o_orderdate-23_p_name-2_p_partkey-2_p_brand-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [['o_orderdate', 23]],
  'part': [('p_name', 2), ('p_partkey', 2), ('p_brand', 1)]},
 'l_shipdate-92_o_orderdate-23_p_name-2_p_partkey-2': {'lineitem': [['l_shipdate',
    92]],
  'orders': [['o_orderdate', 23]],
  'part': [('p_name', 2), ('p_partkey', 2)]},
 'l_shipdate-92_o_orderdate-23_p_brand-1_p_partkey-4_p_brand-1': {'lineitem': [['l_shipdate',
    92]],
  'orders': [['o_orderdate', 23]],
  'part': [('p_brand', 1), ('p_partkey', 4), ('p_brand', 1)]},
 'l_orderkey-11_l_partkey-9_l_shipdate-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    11),
   ('l_partkey', 9),
   ('l_shipdate', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_orderkey-13_l_shipdate-8_l_partkey-1_o_orderdate-23': {'lineitem': [('l_orderkey',
    13),
   ('l_shipdate', 8),
   ('l_partkey', 1)],
  'orders': [['o_orderdate', 23]]},
 'l_partkey-13_l_shipdate-8_l_orderkey-1_o_orderdate-23': {'lineitem': [('l_partkey',
    13),
   ('l_shipdate', 8),
   ('l_orderkey', 1)],
  'orders': [['o_orderdate', 23]]}
}


    # SF 1 test disjoint_clusters
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


