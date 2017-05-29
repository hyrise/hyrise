#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/sql/sql_query_translator.hpp"
#include "../base_fixture.cpp"
#include "SQLParser.h"

namespace opossum {

class SQLBenchmark : public BenchmarkBasicFixture {
 public:
  virtual void SetUp(const ::benchmark::State& state) {}

  virtual void TearDown(const ::benchmark::State& state) {}

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
    SQLQueryTranslator translator;
    translator.translate_query(Q1);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsingQ1)(benchmark::State& state) {
  while (state.KeepRunning()) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parseSQLString(Q1, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTranslationQ1)(benchmark::State& state) {
  hsql::SQLParserResult result;
  hsql::SQLParser::parseSQLString(Q1, &result);

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_statement(*result.getStatement(0));
  }
}

// Q2

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsingQ2)(benchmark::State& state) {
  while (state.KeepRunning()) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parseSQLString(Q2, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTranslationQ2)(benchmark::State& state) {
  hsql::SQLParserResult result;
  hsql::SQLParser::parseSQLString(Q2, &result);

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_statement(*result.getStatement(0));
  }
}

// Q3
BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsingQ3)(benchmark::State& state) {
  while (state.KeepRunning()) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parseSQLString(Q3, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTranslationQ3)(benchmark::State& state) {
  hsql::SQLParserResult result;
  hsql::SQLParser::parseSQLString(Q3, &result);

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_statement(*result.getStatement(0));
  }
}

}  // namespace opossum
