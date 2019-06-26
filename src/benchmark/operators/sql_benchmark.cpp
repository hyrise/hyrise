#include <memory>
#include <string>

#include "../micro_benchmark_basic_fixture.hpp"
#include "SQLParser.h"
#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

using hsql::SQLParser;
using hsql::SQLParserResult;

class SQLBenchmark : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(benchmark::State& st) override {
    // Add tables to StorageManager.
    // This is required for the translator to get the column names of a table.
    auto& storage_manager = Hyrise::get().storage_manager;
    storage_manager.add_table("customer", load_table("resources/test_data/tbl/tpch/minimal/customer.tbl"));
    storage_manager.add_table("lineitem", load_table("resources/test_data/tbl/tpch/minimal/lineitem.tbl"));
    storage_manager.add_table("orders", load_table("resources/test_data/tbl/tpch/minimal/orders.tbl"));
  }

  // Run a benchmark that compiles the given SQL query.
  void BM_CompileQuery(benchmark::State& state) {
    for (auto _ : state) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result)[0];
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that only parses the given SQL query.
  void BM_ParseQuery(benchmark::State& state) {
    for (auto _ : state) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
    }
  }

  // Run a benchmark that only plans the given SQL query.
  void BM_PlanQuery(benchmark::State& state) {
    SQLParserResult result;
    SQLParser::parseSQLString(query, &result);
    for (auto _ : state) {
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result)[0];
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that plans the query operator with the given query with enabled query plan caching.
  void BM_QueryPlanCache(benchmark::State& state) {
    const auto pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

    pqp_cache->resize(16);

    for (auto _ : state) {
      auto pipeline_statement = SQLPipelineBuilder{query}.with_pqp_cache(pqp_cache).create_pipeline_statement();
      pipeline_statement.get_physical_plan();
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

BENCHMARK_F(SQLBenchmark, BM_CompileQuery)(benchmark::State& st) { BM_CompileQuery(st); }
BENCHMARK_F(SQLBenchmark, BM_ParseQuery)(benchmark::State& st) { BM_ParseQuery(st); }
BENCHMARK_F(SQLBenchmark, BM_PlanQuery)(benchmark::State& st) { BM_PlanQuery(st); }
BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQuery)(benchmark::State& st) { BM_QueryPlanCache(st); }

}  // namespace opossum
