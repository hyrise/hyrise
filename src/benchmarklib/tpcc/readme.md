## TPC-C Benchmark Suite for Hyrise

TPC-C is a transactional benchmark framework focusing on queries that typically select or update only a few rows
from a table. The given schema consists of nine tables, namely WAREHOUSE, ITEMS, STOCK, DISTRICT, CUSTOMER, HISTORY,
ORDER, ORDER_LINE, and NEW_ORDER. With all these tables TPC-C represents an enterprise system
that handles incoming orders from customers for items.

The definition of TPC-C provides a set of rules how to implement the benchmark correctly,
such as database population rules or transaction definitions.

### How does Hyrise implement TPC-C

For Hyrise we added a TPC-C Table Generator class (TableGenerator) that uses these database population rules and
generates Hyrise Tables. These tables are then used in the benchmarks to measure the performance of this database given
a set of transactions.


### Cross-validation with SQLite

To make sure that all changes in Hyrise, whether in the optimizer or in any operator,
do not produce any wrong results, we added a validation phase. In this phase we import the previously generated tables
into SQLite and run the same set of transactions.
In the end we compare the results between Hyrise and SQLite hoping they are the same.


### Known limitations

For now we implemented a working, but not complete version of TPC-C. Due to time limitations
we decided to skip some parts of TPC-C, e.g. some transactions. Adding them later on is only a diligence work,
but hopefully does not raise new problems.


#### Subset of transactions

For now we implemented 3 out of 5 transactions that are defined in TPC-C, namely Delivery, New-Order, and Order-Status.
The missing ones are Payment and Stock-Level. A while ago we already started implementing the Payment transaction,
but since it was by far not ready we removed it in commit d9a5236.

For the Order-Status transaction we not only added benchmarks for the whole transaction, but also for single queries.
Since this transaction consists of 4 queries, we end up with 5 benchmarks, resp. 7 benchmarks if you also include
the benchmarks for Delivery and New-Order.


#### Table Setup Overhead

Currently the table generation is invoked in the constructor of the TPC-C benchmark base class (TPCCBenchmarkFixture).
Since all transactions inherit from this class, this constructor is called once for every benchmark.
In the end we currently generate all tables seven times, which is, except for long execution times,
not a problem for the benchmark itself.

Additionally the chunk_size is not optimized yet. Feel free to try different values.


#### Multiple Warehouses

Table sizes in TPC-C are defined as factors of other tables. For example TPC-C states that there are 100 000 stocks
for each warehouse. In general warehouse is the base for all the other table sizes,
so if you want to scale your TPC-C you have to increase the number of warehouses.

For now this implementation assumes there is only one warehouse, which is the default in TPC-C.
Some transactions rely on this assumption to simplify the code for the queries,
but the table generator on the other hand is able to scale with the number of warehouses.

So, if you want to scale, make sure that all benchmarks are able to handle multiple warehouses.


#### Modifying queries not properly tested in cross-validation

In the cross-validation step we compare the results of Hyrise and SQLite for each query. However, we do this only
for SELECT queries, but not for INSERT or UPDATE. The latter ones are implicitly tested by checking the results for
other/later queries.