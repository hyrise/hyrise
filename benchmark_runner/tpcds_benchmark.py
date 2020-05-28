from abstract_benchmark import AbstractBenchmark

class TPCDSBenchmark(AbstractBenchmark):
  def name(self):
  	return "tpcds"

  def exec_path(self):
    return "/home/Alexander.Loeser/hyrise/build-release/hyrisePlayground"

  def result_path(self):
    return "/home/Alexander.Loeser/hyrise/benchmark_results/tpcds/joins_frequency_clusters"

  def time(self):
    return 60

  def max_runs(self):
    return -1

  def scale(self):
    return 1

  def chunk_sizes(self):
    #return [25000, 100000]
    return [65535]

  def sort_orders(self):

  # SF 1 RUNS 1
    return  {'ss_sold_date_sk-6_ss_ticket_number-9_ss_sold_time_sk-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_ticket_number', 9),
   ('ss_sold_time_sk', 1)]},
 'ss_sold_date_sk-6_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 'ss_sold_time_sk-7_ss_ticket_number-8_ss_sold_date_sk-1': {'store_sales': [('ss_sold_time_sk',
    7),
   ('ss_ticket_number', 8),
   ('ss_sold_date_sk', 1)]}}


  # SF 1 RUNS -1
    return {'default': {},
 'cd_demo_sk-13_cd_education_status-3': {'customer_demographics': [('cd_demo_sk',
    13),
   ('cd_education_status', 3)]},
 'cd_demo_sk-18_cd_gender-2_cd_education_status-1': {'customer_demographics': [('cd_demo_sk',
    18),
   ('cd_gender', 2),
   ('cd_education_status', 1)]},
 'cd_demo_sk-15_cd_marital_status-2_cd_education_status-1': {'customer_demographics': [('cd_demo_sk',
    15),
   ('cd_marital_status', 2),
   ('cd_education_status', 1)]},
 'ss_sold_date_sk-6_ss_ticket_number-9_ss_item_sk-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_ticket_number', 9),
   ('ss_item_sk', 1)]},
 'ss_item_sk-8_ss_sold_date_sk-6_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    8),
   ('ss_sold_date_sk', 6),
   ('ss_ticket_number', 1)]},
 'ss_item_sk-6_ss_ticket_number-8_ss_sold_date_sk-1': {'store_sales': [('ss_item_sk',
    6),
   ('ss_ticket_number', 8),
   ('ss_sold_date_sk', 1)]}}


  # SF 1 RUNS -1 
    return  {
 'ss_sold_date_sk-6_ss_ticket_number-9_ss_item_sk-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_ticket_number', 9),
   ('ss_item_sk', 1)]},
 'ss_item_sk-8_ss_sold_date_sk-6_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    8),
   ('ss_sold_date_sk', 6),
   ('ss_ticket_number', 1)]},
 'ss_item_sk-6_ss_ticket_number-8_ss_sold_date_sk-1': {'store_sales': [('ss_item_sk',
    6),
   ('ss_ticket_number', 8),
   ('ss_sold_date_sk', 1)]}}



    return {
      'ss_ticket_number-48': {'store_sales': [['ss_ticket_number', 48]]}
    }

    return {
 'default': {},
 'cd_demo_sk-13_cd_education_status-3': {'customer_demographics': [('cd_demo_sk',
    13),
   ('cd_education_status', 3)]},
 'cd_demo_sk-18_cd_gender-2_cd_education_status-1': {'customer_demographics': [('cd_demo_sk',
    18),
   ('cd_gender', 2),
   ('cd_education_status', 1)]},
 'cd_demo_sk-15_cd_marital_status-2_cd_education_status-1': {'customer_demographics': [('cd_demo_sk',
    15),
   ('cd_marital_status', 2),
   ('cd_education_status', 1)]},
 'ss_sold_date_sk-6_ss_ticket_number-9_ss_sold_time_sk-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_ticket_number', 9),
   ('ss_sold_time_sk', 1)]},
 'ss_sold_date_sk-6_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 'ss_sold_time_sk-7_ss_ticket_number-8_ss_sold_date_sk-1': {'store_sales': [('ss_sold_time_sk',
    7),
   ('ss_ticket_number', 8),
   ('ss_sold_date_sk', 1)]}}


    # SF 1
    return {
      'ss_ticket_number-45_ss_quantity-1': {'store_sales': [('ss_ticket_number', 45), ('ss_quantity', 1)]},
      'ss_net_profit-45_ss_ticket_number-1': {'store_sales': [('ss_net_profit', 45), ('ss_ticket_number', 1)]},
    }    


    return {
      'ss_item_sk-45_ss_quantity-1': {'store_sales': [('ss_item_sk', 45), ('ss_quantity', 1)]},
      'ss_net_profit-45_ss_item_sk-1': {'store_sales': [('ss_net_profit', 45), ('ss_item_sk', 1)]},
    }

    return {
    'default': {},
 'cd_education_status-7_cd_marital_status-5_cd_education_status-1': {'customer_demographics': [('cd_education_status',
   7),
   ('cd_marital_status', 5),
   ('cd_education_status', 1)]},
 'cd_education_status-8_cd_gender-4_cd_education_status-1': {'customer_demographics': [('cd_education_status',
    8),
   ('cd_gender', 4),
   ('cd_education_status', 1)]},
 'cd_education_status-30': {'customer_demographics': [('cd_education_status',
    30)]},
 'ss_net_profit-45_ss_quantity-1': {'store_sales': [('ss_net_profit', 45),
   ('ss_quantity', 1)]},
 'ss_net_profit-11_ss_quantity-5': {'store_sales': [('ss_net_profit', 11),
   ('ss_quantity', 5)]},
 'ss_net_profit-8_ss_wholesale_cost-6_ss_quantity-1': {'store_sales': [('ss_net_profit',
    8),
   ('ss_wholesale_cost', 6),
   ('ss_quantity', 1)]}}


    # SF 10
    return {'default': {},
 'cd_education_status-7_cd_marital_status-5_cd_education_status-1': {'customer_demographics': [('cd_education_status',
    7),
   ('cd_marital_status', 5),
   ('cd_education_status', 1)]},
 'cd_education_status-8_cd_gender-4_cd_education_status-1': {'customer_demographics': [('cd_education_status',
    8),
   ('cd_gender', 4),
   ('cd_education_status', 1)]},
 'cd_education_status-30': {'customer_demographics': [('cd_education_status',
    30)]},
 'ca_gmt_offset-2_ca_state-3_ca_country-1': {'customer_address': [('ca_gmt_offset',
    2),
   ('ca_state', 3),
   ('ca_country', 1)]},
 'ca_gmt_offset-4_ca_country-1': {'customer_address': [('ca_gmt_offset', 4),
   ('ca_country', 1)]},
 'ca_country-1_ca_gmt_offset-4_ca_country-1': {'customer_address': [('ca_country',
    1),
   ('ca_gmt_offset', 4),
   ('ca_country', 1)]},
 'ss_coupon_amt-444_ss_quantity-1': {'store_sales': [('ss_coupon_amt', 444),
   ('ss_quantity', 1)]},
 'ss_coupon_amt-34_ss_quantity-14': {'store_sales': [('ss_coupon_amt', 34),
   ('ss_quantity', 14)]},
 'ss_coupon_amt-26_ss_wholesale_cost-18_ss_quantity-1': {'store_sales': [('ss_coupon_amt',
    26),
   ('ss_wholesale_cost', 18),
   ('ss_quantity', 1)]}}




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
