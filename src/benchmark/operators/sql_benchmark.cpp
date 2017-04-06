#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/sql/sql_query_translator.hpp"
#include "../base_fixture.cpp"
#include "SQLParser.h"

namespace opossum {

class SQLBenchmark : public BenchmarkFixture {
 public:
  virtual void SetUp(const ::benchmark::State& state) {}

  virtual void TearDown(const ::benchmark::State& state) {}
};

BENCHMARK_F(SQLBenchmark, BM_SQLFullQueryExecution)(benchmark::State& state) {
  clear_cache();
  const std::string query = "SELECT * FROM benchmark_table_one ORDER BY a DESC;";

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_query(query);
    auto tasks = translator.get_tasks();

    for (uint i = 0; i < tasks.size(); ++i) {
      tasks[i]->get_operator()->execute();
    }
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationTotal)(benchmark::State& state) {
  clear_cache();
  const std::string query = "SELECT * FROM benchmark_table_one ORDER BY a DESC;";

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_query(query);
  }
}

BENCHMARK_F(SQLBenchmark, BM_SQLTranslationWithoutParsing)(benchmark::State& state) {
  clear_cache();
  const std::string query = "SELECT * FROM benchmark_table_one ORDER BY a DESC;";
  hsql::SQLParserResult result;
  hsql::SQLParser::parseSQLString(query, &result);

  while (state.KeepRunning()) {
    SQLQueryTranslator translator;
    translator.translate_statement(*result.getStatement(0));
  }
}

}  // namespace opossum
