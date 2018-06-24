#include "benchmark/benchmark.h"

#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
//#include "operators/jit_operator/jit_aware_lqp_translator.cpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_statement.hpp"

namespace opossum {

class ExpressionPresentationFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State &state) override {
    TpchDbGenerator{0.1f}.generate_and_store();
  }

  void TearDown(benchmark::State &state) override {
    StorageManager::reset();
  }

  const std::string query_arithmetics = "SELECT l_extendedprice*(1.0-l_discount)*(1.0+l_tax) FROM lineitem";
};

BENCHMARK_F(ExpressionPresentationFixture, Arithmetics)(benchmark::State& state) {
  while (state.KeepRunning()) {
    auto pipeline_statement = SQLPipelineBuilder{query_arithmetics}.disable_mvcc().create_pipeline_statement();
    pipeline_statement.get_result_table();
  }
}

//BENCHMARK_F(ExpressionPresentationFixture, Arithmetics_JIT)(benchmark::State& state) {
//  while (state.KeepRunning()) {
//    auto pipeline_statement = SQLPipelineBuilder{query_arithmetics}.
//      disable_mvcc().
//      with_lqp_translator(std::make_shared<JitAwareLQPTranslator>()).
//      create_pipeline_statement();
//
//    pipeline_statement.get_result_table();
//  }
//}

}  // namespace opossum