#!/usr/bin/python3

import argparse
import atexit
from datetime import datetime
import json
import os
import psycopg2
from pathlib import Path
import random
import subprocess
import sys
import threading
import time

queries = []
queries.append("""SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, SUM(l_extendedprice*(1.0-l_discount)) as sum_disc_price, SUM(l_extendedprice*(1.0-l_discount)*(1.0+l_tax)) as sum_charge, AVG(l_quantity) as avg_qty, AVG(l_extendedprice) as avg_price, AVG(l_discount) as avg_disc, COUNT(*) as count_order FROM lineitem WHERE l_shipdate <= '1998-12-01' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;""")
queries.append("""SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type like '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND ps_supplycost = (SELECT min(ps_supplycost) FROM supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;""")
queries.append("""SELECT l_orderkey, SUM(l_extendedprice*(1.0-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;""")
queries.append("""SELECT o_orderpriority, count(*) as order_count FROM orders WHERE o_orderdate >= '1996-07-01' AND o_orderdate < '1996-10-01' AND exists (SELECT *FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority;""")
queries.append("""SELECT n_name, SUM(l_extendedprice * (1.0 - l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AMERICA' AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01' GROUP BY n_name ORDER BY revenue DESC;""")
queries.append("""SELECT sum(l_extendedprice*l_discount) AS REVENUE FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount BETWEEN .05 AND .07 AND l_quantity < 24;""")
queries.append("""SELECT supp_nation, cust_nation, l_year, SUM(volume) as revenue FROM (SELECT n1.n_name as supp_nation, n2.n_name as cust_nation, SUBSTR(l_shipdate, 1, 4) as l_year, l_extendedprice * (1.0 - l_discount) as volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'IRAN' AND n2.n_name = 'IRAQ') OR (n1.n_name = 'IRAQ' AND n2.n_name = 'IRAN')) AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') as shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;""")
queries.append("""SELECT o_year, SUM(case when nation = 'BRAZIL' then volume else 0 end) / SUM(volume) as mkt_share FROM (SELECT SUBSTR(o_orderdate, 1, 4) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate between '1995-01-01' AND '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL') as all_nations GROUP BY o_year ORDER BY o_year;""")
queries.append("""SELECT nation, o_year, SUM(amount) as sum_profit FROM (SELECT n_name as nation, SUBSTR(o_orderdate, 1, 4) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name like '%green%') as profit GROUP BY nation, o_year ORDER BY nation, o_year DESC;""")
queries.append("""SELECT c_custkey, c_name, SUM(l_extendedprice * (1.0 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20;""")
queries.append("""SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey having SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY value DESC;""")
queries.append("""SELECT l_shipmode, SUM(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, SUM(case when o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL','SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;""")
queries.append("""SELECT c_count, count(*) as custdist FROM (SELECT c_custkey, count(o_orderkey) as c_count FROM customer left outer join orders on c_custkey = o_custkey AND o_comment not like '%special%request%' GROUP BY c_custkey) as c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC;""")
queries.append("""SELECT 100.00 * SUM(case when p_type like 'PROMO%' then l_extendedprice*(1.0-l_discount) else 0 end) / SUM(l_extendedprice * (1.0 - l_discount)) as promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01';""")
queries.append("""create view revenue[stream] (supplier_no, total_revenue) as SELECT l_suppkey, SUM(l_extendedprice * (1.0 - l_discount)) FROM lineitem WHERE l_shipdate >= '1993-05-13' AND l_shipdate < '1993-08-13' GROUP BY l_suppkey; SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue[stream] WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue[stream]) ORDER BY s_suppkey; drop view revenue[stream];""")
queries.append("""SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type not like 'MEDIUM POLISHED%' AND p_size in (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey not in (SELECT s_suppkey FROM supplier WHERE s_comment like '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;""")
queries.append("""SELECT SUM(l_extendedprice) / 7.0 as avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey);""")
queries.append("""SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey in (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey having SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;""")
queries.append("""SELECT SUM(l_extendedprice * (1.0 - l_discount)) as revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size between 1 AND 5 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size between 1 AND 10 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size between 1 AND 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON');""")
queries.append("""SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey in (SELECT ps_suppkey FROM partsupp WHERE ps_partkey in (SELECT p_partkey FROM part WHERE p_name like 'forest%') AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01')) AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name;""")
queries.append("""SELECT s_name, count(*) as numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND exists (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND not exists (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;""")
queries.append("""SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(c_acctbal) AS TOTACCTBAL FROM (SELECT SUBSTR(c_phone,1,2) AS CNTRYCODE, c_acctbal FROM customer WHERE SUBSTR(c_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTR(c_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS CUSTSALE GROUP BY CNTRYCODE ORDER BY CNTRYCODE;""")

parser = argparse.ArgumentParser()
parser.add_argument('server_path', type=str, nargs='?', help='path to the Hyrise server binary (benchmarking plugin expected in lib/)')
parser.add_argument('--time', '-t', type=int, default=300)
parser.add_argument('--port', '-p', type=int, default=5432)
parser.add_argument('--output', '-o', type=str)
parser.add_argument('--clients', type=int, default=1)
parser.add_argument('--cores', type=int, default=1)
parser.add_argument('--scale', '-s', type=float, default=10)

# TODO add -s, print in context, use for choice of DB (Hyrise, too)
args = parser.parse_args()

plugin_filename = list(Path(os.path.join(args.server_path, 'lib')).glob('libBenchmarkingPlugin.*'))[0]
print("Found benchmarking plugin {}".format(plugin_filename))

hyrise_server_process = None
def cleanup():
  print("Shutting {} down Hyrise.")
  hyrise_server_process.kill()
  time.sleep(10)
atexit.register(cleanup)

print("Starting Hyrise server...")
hyrise_server_process = subprocess.Popen(['numactl', '-C', "+0-+{}".format(args.cores - 1), '{}/hyriseServer'.format(args.server_path), '-p', str(args.port), '--benchmark_data=TPC-H:{}'.format(str(args.scale))], stdout=subprocess.PIPE, bufsize=1)
time.sleep(5)
while True:
  line = hyrise_server_process.stdout.readline()
  if b'Server started at' in line:
    break

# todo clients, cores, scheduler
report = {'benchmarks': [], 'context': {'benchmark_mode': 'Ordered', 'build_type': 'Release', 'GIT-HASH': '', 'DBMS': "hyrise", 'chunk_size': 0, 'clients': args.clients, 'compiler': '', 'cores': args.cores, 'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'encoding': '', 'indexes': '', 'max_duration': args.time*1e9, 'max_runs': -1, 'scale_factor': args.scale, 'time_unit': 'ns', 'use_prepared_statements': False, 'using_scheduler': True, 'verify': False, 'warmup_duration': 0}}

def loop(thread_id, query_id, start_time, successful_runs, timeout, is_warmup=False):
  connection = psycopg2.connect("host=localhost port={}".format(args.port))
  cursor = connection.cursor()

  runs = []
  
  if is_warmup:
    for query in queries:
      query = query.replace('[stream]', str(thread_id))
      cursor.execute(query)
      print('.', end="")

    cursor.close()
    connection.close()
    return

  while True:
    item_start_time = time.time()

    if query_id == 'shuffled':
      shuffled_queries = queries.copy()
      random.shuffle(shuffled_queries)
      for query in shuffled_queries:
        query = query.replace('[stream]', str(thread_id))
        cursor.execute(query)

    else:
      query = queries[query_id - 1]
      query = query.replace('[stream]', str(thread_id))
      cursor.execute(query)

    item_end_time = time.time()
    if ((item_end_time - start_time) < timeout):
      runs.append(item_end_time - item_start_time)
    else:
      cursor.close()
      connection.close()
      break

  successful_runs.extend(runs)

print("Warming up (complete single-threaded TPC-H run) ...", end="")
sys.stdout.flush()
loop(0, 'shuffled', time.time(), [], 3600, True)
print(" done.")
sys.stdout.flush()

main_connection = psycopg2.connect("host=localhost port={}".format(args.port))
main_cursor = main_connection.cursor()
main_connection.autocommit = True
main_cursor.execute("INSERT INTO meta_plugins(name) VALUES ('{}');".format(os.path.join(args.server_path, plugin_filename)))

result_csv_filename = 'benchmarking_results__sf_{}__{}_cores__{}_clients.csv'.format(args.scale, args.cores, args.clients)
result_csv = open(result_csv_filename, 'w')
result_csv.write('SCALE_FACTOR,CORES,CLIENTS,ITEM,RADIX_CACHE_USAGE_RATIO,SEMI_JOIN_RATIO,RUNTIME_S\n')

for radix_cache_usage_ratio in [0.1, 0.3, 0.5, 0.7, 0.9, 1.0, 1.1, 1.2]:
  for semi_join_ratio in [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5]:

    print("Running with RadixCacheUsageRatio of {} and SemiJoinRatio of {}.".format(radix_cache_usage_ratio, semi_join_ratio))

    main_cursor.execute("UPDATE meta_settings SET value = '{}' WHERE name = 'Plugin::Benchmarking::RadixCacheUsageRatio'".format(radix_cache_usage_ratio))
    main_cursor.execute("UPDATE meta_settings SET value = '{}' WHERE name = 'Plugin::Benchmarking::SemiJoinRatio'".format(semi_join_ratio))

    for query_id in ['shuffled'] + list(range(1, 23)):
      query_name = 'TPC-H {:02}'.format(query_id) if query_id != 'shuffled' else 'shuffled'
      print('Benchmarking {}...'.format(query_name), end='', flush=True)

      successful_runs = []
      start_time = time.time()

      timeout = args.time if query_id != 'shuffled' else max(3600, args.time)

      threads = []
      for thread_id in range (0, args.clients):
        threads.append(threading.Thread(target=loop, args=(thread_id, query_id, start_time, successful_runs, timeout)))
        threads[-1].start()

      while True:
        time_left = start_time + timeout - time.time()
        if time_left < 0:
          break
        print('\rBenchmarking {}... {:.0f} seconds left'.format(query_name, time_left), end="")
        time.sleep(1)

      while True:
        joined_threads = 0
        for thread_id in range (0, args.clients):
          if not threads[thread_id].is_alive():
            joined_threads += 1

        if joined_threads == args.clients:
          break
        else:
          print('\rBenchmarking {}... waiting for {} more clients to finish'.format(query_name, args.clients - joined_threads), end="")
          time.sleep(1)

      print('\r' + ' ' * 80, end='')
      print('\r{}: {:.04f} iter/s'.format(query_name, len(successful_runs) / timeout))

      for successful_run in successful_runs:
        result_csv.write('{},{},{},{},{},{},{}\n'.format(args.scale, args.cores, args.clients, query_name, radix_cache_usage_ratio, semi_join_ratio, successful_run))

result_csv.close()
main_cursor.close()
main_connection.close()
