#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "SQLParser.h"

#include "../base_fixture.cpp"
#include "optimizer/abstract_syntax_tree/node_operator_translator.hpp"
#include "sql/sql_query_node_translator.hpp"
#include "sql/sql_query_operator.hpp"

namespace opossum {

using hsql::SQLParser;
using hsql::SQLParserResult;

class SQLBenchmark : public BenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {}

  void TearDown(::benchmark::State& state) override {}

  const std::string Q1 = "SELECT * FROM test;";
  const std::string Q2 =
      "SELECT a, b AS address "
      "FROM (SELECT * FROM test WHERE c < 100 AND b > 3) t1 "
      "WHERE a < 10 AND b < 100;";
  const std::string Q3 =
      "SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b "
      "FROM table_a AS \"left\" JOIN table_b AS \"right\" ON \"left\".a = \"right\".a;";
};

// Q1

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationTotalQ1)(benchmark::State& state) {
  while (state.KeepRunning()) {
    SQLParserResult result;
    SQLParser::parseSQLString(Q1, &result);

    SQLQueryNodeTranslator translator;
    auto result_node = translator.translate_parse_result(result)[0];
    NodeOperatorTranslator::get().translate_node(result_node);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsingQ1)(benchmark::State& state) {
  while (state.KeepRunning()) {
    SQLParserResult result;
    SQLParser::parseSQLString(Q1, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTranslationQ1)(benchmark::State& state) {
  SQLParserResult result;
  SQLParser::parseSQLString(Q1, &result);

  while (state.KeepRunning()) {
    SQLQueryNodeTranslator translator;
    auto result_node = translator.translate_parse_result(result)[0];
    NodeOperatorTranslator::get().translate_node(result_node);
  }
}

// Q2

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationTotalQ2)(benchmark::State& state) {
  while (state.KeepRunning()) {
    SQLParserResult result;
    SQLParser::parseSQLString(Q2, &result);

    SQLQueryNodeTranslator translator;
    auto result_node = translator.translate_parse_result(result)[0];
    NodeOperatorTranslator::get().translate_node(result_node);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsingQ2)(benchmark::State& state) {
  while (state.KeepRunning()) {
    SQLParserResult result;
    SQLParser::parseSQLString(Q2, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTranslationQ2)(benchmark::State& state) {
  SQLParserResult result;
  SQLParser::parseSQLString(Q2, &result);

  while (state.KeepRunning()) {
    SQLQueryNodeTranslator translator;
    auto result_node = translator.translate_parse_result(result)[0];
    NodeOperatorTranslator::get().translate_node(result_node);
  }
}

// Q3
BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsingQ3)(benchmark::State& state) {
  while (state.KeepRunning()) {
    SQLParserResult result;
    SQLParser::parseSQLString(Q3, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTranslationQ3)(benchmark::State& state) {
  SQLParserResult result;
  SQLParser::parseSQLString(Q3, &result);

  while (state.KeepRunning()) {
    SQLQueryNodeTranslator translator;
    auto result_node = translator.translate_parse_result(result)[0];
    NodeOperatorTranslator::get().translate_node(result_node);
  }
}

BENCHMARK_DEFINE_F(SQLBenchmark, BM_Q2QueryOperatorWithoutCache)(benchmark::State& state) {
  // Disable cache.
  SQLQueryOperator::get_parse_tree_cache().clear_and_resize(0);
  SQLQueryOperator::get_query_plan_cache().clear_and_resize(0);
  while (state.KeepRunning()) {
    SQLQueryOperator operator_q2(Q2, false);
    operator_q2.execute();
  }
}

BENCHMARK_DEFINE_F(SQLBenchmark, BM_Q2QueryOperatorWithParseTreeCache)(benchmark::State& state) {
  // Enable cache.
  SQLQueryOperator::get_parse_tree_cache().clear_and_resize(16);
  SQLQueryOperator::get_query_plan_cache().clear_and_resize(0);
  while (state.KeepRunning()) {
    SQLQueryOperator operator_q2(Q2, false);
    operator_q2.execute();
  }
}

BENCHMARK_DEFINE_F(SQLBenchmark, BM_Q2QueryOperatorWithQueryPlanCache)(benchmark::State& state) {
  // Enable cache.
  SQLQueryOperator::get_parse_tree_cache().clear_and_resize(0);
  SQLQueryOperator::get_query_plan_cache().clear_and_resize(16);
  while (state.KeepRunning()) {
    SQLQueryOperator operator_q2(Q2, false);
    operator_q2.execute();
  }
}

BENCHMARK_REGISTER_F(SQLBenchmark, BM_Q2QueryOperatorWithoutCache)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(SQLBenchmark, BM_Q2QueryOperatorWithParseTreeCache)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(SQLBenchmark, BM_Q2QueryOperatorWithQueryPlanCache)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
