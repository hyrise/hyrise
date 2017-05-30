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
};

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationTotal)(benchmark::State& state) {
  clear_cache();
  const std::string query = "SELECT * FROM benchmark_table_one WHERE a >= 7;";

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_query(query);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyParsing)(benchmark::State& state) {
  clear_cache();
  const std::string query = "SELECT * FROM benchmark_table_one WHERE a >= 7;";

  while (state.KeepRunning()) {
    hsql::SQLParserResult result;
    hsql::SQLParser::parseSQLString(query, &result);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationOnlyTransation)(benchmark::State& state) {
  clear_cache();
  const std::string query = "SELECT * FROM benchmark_table_one WHERE a >= 7;";
  hsql::SQLParserResult result;
  hsql::SQLParser::parseSQLString(query, &result);

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_statement(*result.getStatement(0));
  }
}

}  // namespace opossum
