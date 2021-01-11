from abstract_benchmark import AbstractBenchmark

class TPCDSBenchmark(AbstractBenchmark):
  def name(self):
  	return "tpcds"

  def visualization_pattern(self):
    return "*-*QP.svg"

  def exec_path(self):
    #return "/home/Alexander.Loeser/hyrise/build-release/hyrisePlayground"
    return "build-release/hyriseCalibration"

  def result_path(self):
    return "/home/Alexander.Loeser/hyrise/benchmark_results/final/tpcds/sf10-3d-corrected"

  def time(self):
    return 500

  def max_runs(self):
    return 100

  def scale(self):
    return 1

  def chunk_sizes(self):
    #return [25000, 100000]
    return [65535]

  def sort_orders(self):

    return {
      'ss_ticket_number-2': {
        'store_sales': [['ss_ticket_number', 2]]
      },
    }

    # final store sales top 20 3d replacements
    return {
  '21-ss_sold_date_sk-8_ss_cdemo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    8),
   ('ss_cdemo_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '22-ss_sold_date_sk-8_ss_promo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    8),
   ('ss_promo_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '23-ss_sold_date_sk-8_ss_sold_time_sk-8_ss_ticket_number-8': {'store_sales': [('ss_sold_date_sk',
    8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 8)]},
    }


    # final store sales top 20 3d corrections
    return {
 '17-ss_sold_date_sk-8_ss_hdemo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    8),
   ('ss_hdemo_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '19-ss_sold_date_sk-8_ss_customer_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    8),
   ('ss_customer_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '20-ss_sold_date_sk-8_ss_store_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    8),
   ('ss_store_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
    }

    # final stores sales top 20 3d clusterings
    return {

 #'01-ss_item_sk-8_ss_addr_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_addr_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'02-ss_sold_date_sk-8_ss_item_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
 #   8),
 #  ('ss_item_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'03-ss_item_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'04-ss_item_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'05-ss_item_sk-8_ss_hdemo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_hdemo_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'06-ss_item_sk-8_ss_customer_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_customer_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'07-ss_item_sk-8_ss_store_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_store_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'08-ss_item_sk-8_ss_cdemo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_cdemo_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'09-ss_item_sk-8_ss_promo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_promo_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 #'10-ss_item_sk-8_ss_sold_time_sk-8_ss_ticket_number-8': {'store_sales': [('ss_item_sk',
 #   8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 8)]},
 #'11-ss_addr_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_addr_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'12-ss_addr_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_addr_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'13-ss_sold_date_sk-8_ss_addr_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
 #   8),
 #  ('ss_addr_sk', 8),
 #  ('ss_sold_time_sk', 8),
 #  ('ss_ticket_number', 1)]},
 '14-ss_hdemo_sk-8_ss_addr_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_hdemo_sk',
    8),
   ('ss_addr_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '15-ss_customer_sk-8_ss_addr_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_customer_sk',
    8),
   ('ss_addr_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '16-ss_store_sk-8_ss_addr_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_store_sk',
    8),
   ('ss_addr_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '17-ss_addr_sk-8_ss_cdemo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_addr_sk',
    8),
   ('ss_cdemo_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '18-ss_addr_sk-8_ss_promo_sk-8_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_addr_sk',
    8),
   ('ss_promo_sk', 8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},
 '19-ss_addr_sk-8_ss_sold_time_sk-8_ss_ticket_number-8': {'store_sales': [('ss_addr_sk',
    8),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 8)]},
 '20-ss_sold_date_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    21),
   ('ss_sold_time_sk', 21),
   ('ss_ticket_number', 1)]}}


    # final top 20 SF
    return {
    #'00-nosort':{'store_sales': [['ss_ticket_number', 2]]},

    #'01-ss_item_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    #21),
   #('ss_sold_time_sk', 21),
   #('ss_ticket_number', 1)]},
 #'02-ss_addr_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_addr_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'03-ss_sold_date_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'04-ss_sold_time_sk-100_ss_ticket_number-1': {'store_sales': [('ss_sold_time_sk',
 #   100),
 #  ('ss_ticket_number', 1)]},
 #'05-ss_customer_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_customer_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'06-ss_hdemo_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_hdemo_sk',
 #  21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'07-ss_cdemo_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_cdemo_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'08-ss_store_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_store_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'09-ss_promo_sk-21_ss_sold_time_sk-21_ss_ticket_number-1': {'store_sales': [('ss_promo_sk',
 #   21),
 #  ('ss_sold_time_sk', 21),
 #  ('ss_ticket_number', 1)]},
 #'10-ss_sold_time_sk-21_ss_ticket_number-21': {'store_sales': [('ss_sold_time_sk',
 #   21),
 #  ('ss_ticket_number', 21)]},
 '11-ss_ticket_number-100': {'store_sales': [('ss_ticket_number', 100)]},
 '12-ss_item_sk-21_ss_addr_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    21),
   ('ss_addr_sk', 21),
   ('ss_ticket_number', 1)]},
 '13-ss_sold_date_sk-21_ss_item_sk-21_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    21),
   ('ss_item_sk', 21),
   ('ss_ticket_number', 1)]},
 '14-ss_item_sk-100_ss_ticket_number-1': {'store_sales': [('ss_item_sk', 100),
   ('ss_ticket_number', 1)]},
 '15-ss_item_sk-21_ss_customer_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    21),
   ('ss_customer_sk', 21),
   ('ss_ticket_number', 1)]},
 '16-ss_item_sk-21_ss_hdemo_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    21),
   ('ss_hdemo_sk', 21),
   ('ss_ticket_number', 1)]},
 '17-ss_item_sk-21_ss_cdemo_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    21),
   ('ss_cdemo_sk', 21),
   ('ss_ticket_number', 1)]},
 '18-ss_item_sk-21_ss_store_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    21),
   ('ss_store_sk', 21),
   ('ss_ticket_number', 1)]},
 '19-ss_item_sk-21_ss_promo_sk-21_ss_ticket_number-1': {'store_sales': [('ss_item_sk',
    21),
   ('ss_promo_sk', 21),
   ('ss_ticket_number', 1)]},
 '20-ss_item_sk-21_ss_ticket_number-21': {'store_sales': [('ss_item_sk', 21),
   ('ss_ticket_number', 21)]}}



    # tmp
    return {
      'default':{},
      'ss_sold_date_sk-6_ss_sold_time_sk-8_ss_ticket_number-1': {'store_sales': [('ss_sold_date_sk',
    6),
   ('ss_sold_time_sk', 8),
   ('ss_ticket_number', 1)]},

    }

    return {
      'sr_ticket_number-5_sr_reason_sk-1': {'store_returns': [('sr_ticket_number',
    5),
   ('sr_reason_sk', 1)]},
 'sr_customer_sk-3_sr_ticket_number-3_sr_reason_sk-1': {'store_returns': [('sr_customer_sk',
    3),
   ('sr_ticket_number', 3),
   ('sr_reason_sk', 1)]},
 'sr_returned_date_sk-3_sr_ticket_number-3_sr_reason_sk-1': {'store_returns': [('sr_returned_date_sk',
    3),
   ('sr_ticket_number', 3),
   ('sr_reason_sk', 1)]},
 'ws_sold_date_sk-4_ws_bill_addr_sk-4_ws_sold_date_sk-1': {'web_sales': [('ws_sold_date_sk',
    4),
   ('ws_bill_addr_sk', 4),
   ('ws_sold_date_sk', 1)]},
 'ws_bill_addr_sk-4_ws_bill_customer_sk-4_ws_sold_date_sk-1': {'web_sales': [('ws_bill_addr_sk',
    4),
   ('ws_bill_customer_sk', 4),
   ('ws_sold_date_sk', 1)]},
 'ws_bill_addr_sk-4_ws_item_sk-4_ws_sold_date_sk-1': {'web_sales': [('ws_bill_addr_sk',
    4),
   ('ws_item_sk', 4),
   ('ws_sold_date_sk', 1)]},
 'cs_sold_date_sk-5_cs_bill_cdemo_sk-5_cs_sold_date_sk-1': {'catalog_sales': [('cs_sold_date_sk',
    5),
   ('cs_bill_cdemo_sk', 5),
   ('cs_sold_date_sk', 1)]},
 'cs_sold_date_sk-5_cs_bill_customer_sk-5_cs_sold_date_sk-1': {'catalog_sales': [('cs_sold_date_sk',
    5),
   ('cs_bill_customer_sk', 5),
   ('cs_sold_date_sk', 1)]},
 'cs_bill_customer_sk-5_cs_bill_cdemo_sk-5_cs_sold_date_sk-1': {'catalog_sales': [('cs_bill_customer_sk',
    5),
   ('cs_bill_cdemo_sk', 5),
   ('cs_sold_date_sk', 1)]},
    }

    return {
      'inv_date_sk-15_inv_item_sk-15_inv_date_sk-1': {
        'inventory': [['inv_date_sk', 15], ['inv_item_sk', 15], ['inv_date_sk', 1]]
      },
      'inv_date_sk-15_inv_item_sk-15_inv_warehouse_sk-1': {
        'inventory': [['inv_date_sk', 15], ['inv_item_sk', 15], ['inv_warehouse_sk', 1]]
      },
      'inv_item_sk-100_inv_date_sk-2': {
        'inventory': [['inv_item_sk', 100], ['inv_date_sk', 2]]
      },
    }

  # SF 1 RUNS -1
    return {
      'nosort': {},
      'inv_item_sk-2': {
        'inventory': [['inv_item_sk', 2]]
      },
      'inv_warehouse_sk-2': {
        'inventory': [['inv_warehouse_sk', 2]]
      },
    }



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
