#include <iostream>
#include <memory>

#include "benchmark/benchmark.h"
#include "benchmark_config.hpp"
#include "cost_model/cost_model_logical.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"
#include "optimizer/strategy/expression_reduction_rule.hpp"
#include "optimizer/strategy/index_scan_rule.hpp"
#include "optimizer/strategy/insert_limit_in_exists.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"
#include "optimizer/strategy/predicate_split_up_rule.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

namespace opossum {

//  const auto query_string = R"(
//    SELECT *
//    FROM T1
//    WHERE T1.id IN (
//      SELECT T2.id
//      FROM T2
//      WHERE T2.id < 500
//    )
//  )";

const auto query_string = R"(
  SELECT *
  FROM T1
  WHERE EXISTS (
    SELECT *
    FROM T2
    WHERE T2.id < 500
    AND T1.id == T2.id
  )
)";

//  const auto query_string = R"(
//    SELECT *
//    FROM T1
//    WHERE T1.id < (
//      SELECT AVG(T2.id)
//      FROM T2
//    )
//  )";

void generate_data(int n1, int n2) {
  auto& storage_manager = StorageManager::get();

  if (storage_manager.has_table("T1")) {
    return;
  }

  auto column_definitions_T1 = TableColumnDefinitions();
  column_definitions_T1.emplace_back("id", DataType::Int);
  auto T1 = std::make_shared<Table>(column_definitions_T1, TableType::Data, std::nullopt, UseMvcc::Yes);
  for (int i = 0; i < n1; i++) {
    T1->append({i});
  }
  Assert(T1->row_count() == static_cast<uint64_t>(n1), "T1 should have n1 rows");
  storage_manager.add_table("T1", T1);

  auto column_definitions_T2 = TableColumnDefinitions();
  column_definitions_T2.emplace_back("id", DataType::Int);
  auto T2 = std::make_shared<Table>(column_definitions_T2, TableType::Data, std::nullopt, UseMvcc::Yes);
  for (int i = 0; i < n2; i++) {
    T2->append({i});
  }
  Assert(T2->row_count() == static_cast<uint64_t>(n2), "T2 should have n2 rows");
  storage_manager.add_table("T2", T2);
}

void execute_pqp(const std::shared_ptr<AbstractOperator>& pqp) {
  auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
  for (auto& task : tasks) {
    task->schedule();
  }
  benchmark::DoNotOptimize(tasks.back()->get_operator()->get_output());
}

class SubqueryToJoinFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    _clear_cache();

    if (!modified_optimizer) {
      // cannot remove rules from optimizer, so add all other rules (see Optimizer::create_default_optimizer() )
      modified_optimizer = std::make_shared<Optimizer>();
      modified_optimizer->add_rule(std::make_unique<ExpressionReductionRule>());
      modified_optimizer->add_rule(std::make_unique<PredicateSplitUpRule>());
      // modified_optimizer->add_rule(std::make_unique<SubqueryToJoinRule>());
      modified_optimizer->add_rule(std::make_unique<ColumnPruningRule>());
      modified_optimizer->add_rule(std::make_unique<InsertLimitInExistsRule>());
      modified_optimizer->add_rule(std::make_unique<ChunkPruningRule>());
      modified_optimizer->add_rule(std::make_unique<JoinOrderingRule>(std::make_unique<CostModelLogical>()));
      modified_optimizer->add_rule(std::make_unique<PredicatePlacementRule>());
      modified_optimizer->add_rule(std::make_unique<PredicateReorderingRule>());
      modified_optimizer->add_rule(std::make_unique<IndexScanRule>());
    }

    if (!nullContext) {
      nullContext = std::make_shared<TransactionContext>(0, 0);
    }
  }

  void TearDown(::benchmark::State& state) override { MicroBenchmarkBasicFixture::TearDown(state); }

 protected:
  std::shared_ptr<Optimizer> modified_optimizer;  // default optimizer without SubqueryToJoinRule
  std::shared_ptr<TransactionContext> nullContext;
};

BENCHMARK_DEFINE_F(SubqueryToJoinFixture, with_subquery_to_join_reformulation)(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    generate_data(static_cast<int>(state.range(0)), static_cast<int>(state.range(1)));
    state.ResumeTiming();

    auto sql_pipeline = SQLPipelineBuilder{query_string}.create_pipeline_statement();
    const auto pqp = LQPTranslator{}.translate_node(sql_pipeline.get_optimized_logical_plan());
    pqp->set_transaction_context_recursively(nullContext);  // validate will fail if context is not set
    execute_pqp(pqp);
  }
}
BENCHMARK_REGISTER_F(SubqueryToJoinFixture, with_subquery_to_join_reformulation)
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000})
    ->Args({100, 10})
    ->Args({100, 100})
    ->Args({100, 1000})
    ->Args({1000, 10})
    ->Args({1000, 100})
    ->Args({1000, 1000});

BENCHMARK_DEFINE_F(SubqueryToJoinFixture, without_subquery_to_join_reformulation)(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    generate_data(static_cast<int>(state.range(0)), static_cast<int>(state.range(1)));
    state.ResumeTiming();

    auto sql_pipeline = SQLPipelineBuilder{query_string}.with_optimizer(modified_optimizer).create_pipeline_statement();
    const auto pqp = LQPTranslator{}.translate_node(sql_pipeline.get_optimized_logical_plan());
    pqp->set_transaction_context_recursively(nullContext);  // validate will fail if context is not set
    execute_pqp(pqp);
  }
}
BENCHMARK_REGISTER_F(SubqueryToJoinFixture, without_subquery_to_join_reformulation)
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000})
    ->Args({100, 10})
    ->Args({100, 100})
    ->Args({100, 1000})
    ->Args({1000, 10})
    ->Args({1000, 100})
    ->Args({1000, 1000});

}  // namespace opossum
