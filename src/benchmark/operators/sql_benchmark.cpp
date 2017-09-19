#include <memory>
#include <string>

#include "SQLParser.h"
#include "benchmark/benchmark.h"

#include "../base_fixture.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "sql/sql_query_operator.hpp"
#include "sql/sql_to_ast_translator.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

namespace opossum {

using hsql::SQLParser;
using hsql::SQLParserResult;

class SQLBenchmark : public BenchmarkBasicFixture {
 public:
  void SetUp(benchmark::State& st) override {
    // Disable and clear all SQL caches.
    SQLQueryOperator::get_parse_tree_cache().clear_and_resize(0);
    SQLQueryOperator::get_query_plan_cache().clear_and_resize(0);
    SQLQueryOperator::get_prepared_statement_cache().clear();

    // Add tables to StorageManager.
    // This is required for the translator to get the column names of a table.
    auto& storage_manager = StorageManager::get();
    storage_manager.add_table("customer", load_table("src/test/tables/tpch/customer.tbl", 0));
    storage_manager.add_table("lineitem", load_table("src/test/tables/tpch/lineitem.tbl", 0));
    storage_manager.add_table("orders", load_table("src/test/tables/tpch/orders.tbl", 0));
  }

  void TearDown(benchmark::State& st) override {}

  // Run a benchmark that compiles the given SQL query.
  void BM_CompileQuery(benchmark::State& st, const std::string& query) {
    while (st.KeepRunning()) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
      auto result_node = SQLToASTTranslator::get().translate_parse_result(result)[0];
      ASTToOperatorTranslator::get().translate_node(result_node);
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
      auto result_node = SQLToASTTranslator::get().translate_parse_result(result)[0];
      ASTToOperatorTranslator::get().translate_node(result_node);
    }
  }

  // Run a benchmark that executes the query operator with the given query.
  void BM_SQLOperatorQuery(benchmark::State& st, const std::string& query) {
    while (st.KeepRunning()) {
      SQLQueryOperator op(query, false);
      op.execute();
    }
  }

  // Run a benchmark that creates a prepared statement with the given query
  // and measures the execution time of the prepared statement.
  void BM_PrepareAndExecute(benchmark::State& st, const std::string& query, const std::string& exec_query) {
    const std::string prepare = "PREPARE cached_query FROM '" + query + "'";
    SQLQueryOperator op(prepare, false);
    op.execute();

    while (st.KeepRunning()) {
      SQLQueryOperator op(exec_query, false);
      op.execute();
    }
  }

  // Run a benchmark that executes the query operator with the given query with enabled parse tree caching.
  void BM_ParseTreeCache(benchmark::State& st, const std::string& query) {
    // Enable parse tree cache.
    SQLQueryOperator::get_parse_tree_cache().clear_and_resize(16);

    while (st.KeepRunning()) {
      SQLQueryOperator op(query, false);
      op.execute();
    }
  }

  // Run a benchmark that executes the query operator with the given query with enabled query plan caching.
  void BM_QueryPlanCache(benchmark::State& st, const std::string& query) {
    // Enable query plan cache.
    SQLQueryOperator::get_query_plan_cache().clear_and_resize(16);

    while (st.KeepRunning()) {
      SQLQueryOperator op(query, false);
      op.execute();
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

  // TODO(anybody): enable once subquery aliases are supported
  //    const std::string Q4 =
  //      R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.orders.o_orderkey)
  //          FROM customer
  //          JOIN (SELECT * FROM
  //            orders
  //            JOIN lineitem ON o_orderkey = l_orderkey
  //          ) AS orderitems ON c_custkey = orders.o_custkey
  //          GROUP BY customer.c_custkey, customer.c_name
  //          HAVING COUNT(orderitems.orders.o_orderkey) >= 100;)";
  //
  //    const std::string Q4Param =
  //      R"("SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.orders.o_orderkey)
  //          FROM customer
  //          JOIN (SELECT * FROM
  //            orders
  //            JOIN lineitem ON o_orderkey = l_orderkey
  //          ) AS orderitems ON c_custkey = orders.o_custkey
  //          GROUP BY customer.c_custkey, customer.c_name
  //          HAVING COUNT(orderitems.orders.o_orderkey) >= ?;)";
};

// Run all benchmarks for Q1.
BENCHMARK_F(SQLBenchmark, BM_CompileQ1)(benchmark::State& st) { BM_CompileQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ1)(benchmark::State& st) { BM_ParseQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ1)(benchmark::State& st) { BM_PlanQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_SQLOperatorQ1)(benchmark::State& st) { BM_SQLOperatorQuery(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_PrepareExecuteQ1)(benchmark::State& st) { BM_PrepareAndExecute(st, Q1, QExec); }
BENCHMARK_F(SQLBenchmark, BM_ParseTreeCacheQ1)(benchmark::State& st) { BM_ParseTreeCache(st, Q1); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ1)(benchmark::State& st) { BM_QueryPlanCache(st, Q1); }

// Run all benchmarks for Q2.
BENCHMARK_F(SQLBenchmark, BM_CompileQ2)(benchmark::State& st) { BM_CompileQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ2)(benchmark::State& st) { BM_ParseQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ2)(benchmark::State& st) { BM_PlanQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_SQLOperatorQ2)(benchmark::State& st) { BM_SQLOperatorQuery(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_PrepareExecuteQ2)(benchmark::State& st) { BM_PrepareAndExecute(st, Q2, QExec); }
BENCHMARK_F(SQLBenchmark, BM_ParseTreeCacheQ2)(benchmark::State& st) { BM_ParseTreeCache(st, Q2); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ2)(benchmark::State& st) { BM_QueryPlanCache(st, Q2); }

// Run all benchmarks for Q3.
BENCHMARK_F(SQLBenchmark, BM_CompileQ3)(benchmark::State& st) { BM_CompileQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_ParseQ3)(benchmark::State& st) { BM_ParseQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_PlanQ3)(benchmark::State& st) { BM_PlanQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_SQLOperatorQ3)(benchmark::State& st) { BM_SQLOperatorQuery(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_PrepareExecuteQ3)(benchmark::State& st) { BM_PrepareAndExecute(st, Q3, QExec); }
BENCHMARK_F(SQLBenchmark, BM_ParseTreeCacheQ3)(benchmark::State& st) { BM_ParseTreeCache(st, Q3); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ3)(benchmark::State& st) { BM_QueryPlanCache(st, Q3); }

// TODO(anybody): enable once subquery aliases are supported
// Run all benchmarks for Q4.
// BENCHMARK_F(SQLBenchmark, BM_CompileQ4)(benchmark::State& st) { BM_CompileQuery(st, Q4); }
// BENCHMARK_F(SQLBenchmark, BM_ParseQ4)(benchmark::State& st) { BM_ParseQuery(st, Q4); }
// BENCHMARK_F(SQLBenchmark, BM_PlanQ4)(benchmark::State& st) { BM_PlanQuery(st, Q4); }
// BENCHMARK_F(SQLBenchmark, BM_SQLOperatorQ4)(benchmark::State& st) { BM_SQLOperatorQuery(st, Q4); }
// BENCHMARK_F(SQLBenchmark, BM_PrepareExecuteQ4)(benchmark::State& st) { BM_PrepareAndExecute(st, Q4, QExec); }
// BENCHMARK_F(SQLBenchmark, BM_ParseTreeCacheQ4)(benchmark::State& st) { BM_ParseTreeCache(st, Q4); }
// BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQ4)(benchmark::State& st) { BM_QueryPlanCache(st, Q4); }

// Benchmark the parsing time of the EXECUTE statement.
BENCHMARK_F(SQLBenchmark, BM_ParseQExec)(benchmark::State& st) { BM_ParseQuery(st, QExec); }

// Parameterized Prepared Statements.
BENCHMARK_F(SQLBenchmark, BM_ParseQExecParam)(benchmark::State& st) { BM_ParseQuery(st, QExecParam); }
// TODO(mp): enable once translator uses indices for operators
// BENCHMARK_F(SQLBenchmark, BM_PrepareExecuteQ4Param)(benchmark::State& st) {
//  BM_PrepareAndExecute(st, Q4Param, QExecParam);
// }

}  // namespace opossum
