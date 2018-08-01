#include <memory>
#include <string>

#include "../benchmark_basic_fixture.hpp"
#include "SQLParser.h"
#include "benchmark/benchmark.h"
#include "logical_query_plan/lqp_translator.hpp"
#include "sql/sql_pipeline_builder.hpp"
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
  void BM_CompileQuery(benchmark::State& st, const std::string& query) {  // NOLINT
    while (st.KeepRunning()) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result)[0];
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that only parses the given SQL query.
  void BM_ParseQuery(benchmark::State& st, const std::string& query) {  // NOLINT
    while (st.KeepRunning()) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
    }
  }

  // Run a benchmark that only plans the given SQL query.
  void BM_PlanQuery(benchmark::State& st, const std::string& query) {  // NOLINT
    SQLParserResult result;
    SQLParser::parseSQLString(query, &result);
    while (st.KeepRunning()) {
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result)[0];
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that plans the query operator with the given query with enabled query plan caching.
  void BM_QueryPlanCache(benchmark::State& st, const std::string& query) {  // NOLINT
    // Enable query plan cache.
    SQLQueryCache<SQLQueryPlan>::get().clear();
    SQLQueryCache<SQLQueryPlan>::get().resize(16);

    while (st.KeepRunning()) {
      auto pipeline_statement = SQLPipelineBuilder{query}.create_pipeline_statement();
      pipeline_statement.get_query_plan();
    }
  }

  const std::string query =
      R"(SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.o_orderkey)
        FROM customer
        JOIN (SELECT * FROM
          orders
          JOIN lineitem ON o_orderkey = l_orderkey
        ) AS orderitems ON c_custkey = orderitems.o_custkey
        GROUP BY customer.c_custkey, customer.c_name
        HAVING COUNT(orderitems.o_orderkey) >= 100;)";
};

BENCHMARK_F(SQLBenchmark, BM_CompileQuery)(benchmark::State& st) { BM_CompileQuery(st, query); }
BENCHMARK_F(SQLBenchmark, BM_ParseQuery)(benchmark::State& st) { BM_ParseQuery(st, query); }
BENCHMARK_F(SQLBenchmark, BM_PlanQuery)(benchmark::State& st) { BM_PlanQuery(st, query); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQuery)(benchmark::State& st) { BM_QueryPlanCache(st, query); }

}  // namespace opossum
