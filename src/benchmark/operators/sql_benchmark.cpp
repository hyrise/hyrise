#include <memory>
#include <string>

#include "../benchmark_basic_fixture.hpp"
#include "SQLParser.h"
#include "benchmark/benchmark.h"
#include "logical_query_plan/lqp_translator.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

namespace opossum {

using hsql::SQLParser;
using hsql::SQLParserResult;

class SQLBenchmark : public BenchmarkBasicFixture {
 public:
  void SetUp(benchmark::State& st) override {
    // Disable and clear all SQL caches.
    SQLQueryCache<SQLQueryPlan>::get().resize(0);

    // Add tables to StorageManager.
    // This is required for the translator to get the column names of a table.
    auto& storage_manager = StorageManager::get();
    storage_manager.add_table("customer", load_table("src/test/tables/tpch/minimal/customer.tbl", Chunk::MAX_SIZE));
    storage_manager.add_table("lineitem", load_table("src/test/tables/tpch/minimal/lineitem.tbl", Chunk::MAX_SIZE));
    storage_manager.add_table("orders", load_table("src/test/tables/tpch/minimal/orders.tbl", Chunk::MAX_SIZE));
  }

  // Run a benchmark that compiles the given SQL query.
  void BM_CompileQuery(benchmark::State& st, const std::string& query) {
    while (st.KeepRunning()) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
      auto result_node = SQLTranslator{false}.translate_parse_result(result)[0];
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that only parses the given SQL query.
  void BM_ParseQuery(benchmark::State& st, const std::string& query) {
    while (st.KeepRunning()) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
    }
  }

  // Run a benchmark that only plans the given SQL query.
  void BM_PlanQuery(benchmark::State& st, const std::string& query) {
    SQLParserResult result;
    SQLParser::parseSQLString(query, &result);
    while (st.KeepRunning()) {
      auto result_node = SQLTranslator{false}.translate_parse_result(result)[0];
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that plans the query operator with the given query with enabled query plan caching.
  void BM_QueryPlanCache(benchmark::State& st, const std::string& query) {
    // Enable query plan cache.
    SQLQueryCache<SQLQueryPlan>::get().clear();
    SQLQueryCache<SQLQueryPlan>::get().resize(16);

    while (st.KeepRunning()) {
      SQLPipelineStatement pipeline_statement{query};
      pipeline_statement.get_query_plan();
    }
  }

  // List of the queries used in benchmarks.

  const std::string QExec = "EXECUTE cached_query;";

  const std::string QExecParam = "EXECUTE cached_query(50);";

  const std::string Q1 = "SELECT * FROM customer;";

  const std::string Q2 =
      "SELECT c_name, c_custkey"
      "  FROM (SELECT * FROM customer WHERE c_custkey < 100 AND c_nationkey=0) t1"
      "  WHERE c_custkey > 10 AND c_nationkey < 10;";

  const std::string Q3 =
      "SELECT c_custkey, c_name, COUNT(o_orderkey)"
      "  FROM customer"
      "  JOIN orders ON c_custkey = o_custkey"
      "  GROUP BY c_custkey, c_name"
      "  HAVING COUNT(o_orderkey) >= 100;";

  const std::string Q4 =
      R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.o_orderkey)
        FROM customer
        JOIN (SELECT * FROM
          orders
          JOIN lineitem ON o_orderkey = l_orderkey
        ) AS orderitems ON c_custkey = orderitems.o_custkey
        GROUP BY customer.c_custkey, customer.c_name
        HAVING COUNT(orderitems.o_orderkey) >= 100;)";

  const std::string Q4Param =
      R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.o_orderkey)
        FROM customer
        JOIN (SELECT * FROM
          orders
          JOIN lineitem ON o_orderkey = l_orderkey
        ) AS orderitems ON c_custkey = orderitems.o_custkey
        GROUP BY customer.c_custkey, customer.c_name
        HAVING COUNT(orderitems.o_orderkey) >= ?;)";
};

// Run all benchmarks for Q1.
BENCHMARK_F(SQLBenchmark, BM_CompileQ1)(benchmark::State& st) { BM_CompileQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ1)(benchmark::State& st) { BM_ParseQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ1)(benchmark::State& st) { BM_PlanQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ1)(benchmark::State& st) { BM_QueryPlanCache(st, Q1); }

// Run all benchmarks for Q2.
BENCHMARK_F(SQLBenchmark, BM_CompileQ2)(benchmark::State& st) { BM_CompileQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ2)(benchmark::State& st) { BM_ParseQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ2)(benchmark::State& st) { BM_PlanQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ2)(benchmark::State& st) { BM_QueryPlanCache(st, Q2); }

// Run all benchmarks for Q3.
BENCHMARK_F(SQLBenchmark, BM_CompileQ3)(benchmark::State& st) { BM_CompileQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ3)(benchmark::State& st) { BM_ParseQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ3)(benchmark::State& st) { BM_PlanQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ3)(benchmark::State& st) { BM_QueryPlanCache(st, Q3); }

// Run all benchmarks for Q4.
BENCHMARK_F(SQLBenchmark, BM_CompileQ4)(benchmark::State& st) { BM_CompileQuery(st, Q4); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ4)(benchmark::State& st) { BM_ParseQuery(st, Q4); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ4)(benchmark::State& st) { BM_PlanQuery(st, Q4); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ4)(benchmark::State& st) { BM_QueryPlanCache(st, Q4); }

}  // namespace opossum
