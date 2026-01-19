#include <memory>
#include <string>

#include "benchmark/benchmark.h"
#include "SQLParser.h"
#include "SQLParserResult.h"

#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

using hsql::SQLParser;
using hsql::SQLParserResult;

class SQLBenchmark : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(benchmark::State& /*st*/) override {
    // Add tables to StorageManager.
    // This is required for the translator to get the column names of a table.
    auto& storage_manager = Hyrise::get().storage_manager;
    storage_manager.add_table("customer", load_table("resources/test_data/tbl/tpch/minimal/customer.tbl"));
    storage_manager.add_table("lineitem", load_table("resources/test_data/tbl/tpch/minimal/lineitem.tbl"));
    storage_manager.add_table("orders", load_table("resources/test_data/tbl/tpch/minimal/orders.tbl"));
  }

  // Run a benchmark that compiles the given SQL query.
  void bm_compile_query(benchmark::State& state) {
    for (auto _ : state) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result).lqp_nodes.at(0);
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that only parses the given SQL query.
  void bm_parse_query(benchmark::State& state) {
    for (auto _ : state) {
      SQLParserResult result;
      SQLParser::parseSQLString(query, &result);
    }
  }

  // Run a benchmark that only plans the given SQL query.
  void bm_plan_query(benchmark::State& state) {
    SQLParserResult result;
    SQLParser::parseSQLString(query, &result);
    for (auto _ : state) {
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result).lqp_nodes.at(0);
      LQPTranslator{}.translate_node(result_node);
    }
  }

  // Run a benchmark that plans the query operator with the given query with enabled query plan caching.
  void bm_query_plan_cache(benchmark::State& state) {
    const auto pqp_cache = std::make_shared<SQLPhysicalPlanCache>();

    pqp_cache->resize(16);

    for (auto _ : state) {
      auto pipeline_statement = SQLPipelineBuilder{query}.with_pqp_cache(pqp_cache).create_pipeline();
      pipeline_statement.get_physical_plans();
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

BENCHMARK_F(SQLBenchmark, BM_CompileQuery)(benchmark::State& state) {
  bm_compile_query(state);
}

BENCHMARK_F(SQLBenchmark, BM_ParseQuery)(benchmark::State& state) {
  bm_parse_query(state);
}

BENCHMARK_F(SQLBenchmark, BM_PlanQuery)(benchmark::State& state) {
  bm_plan_query(state);
}

BENCHMARK_F(SQLBenchmark, BM_QueryPlanCacheQuery)(benchmark::State& state) {
  bm_query_plan_cache(state);
}

}  // namespace hyrise
