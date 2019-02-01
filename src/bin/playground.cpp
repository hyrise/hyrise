#include <fstream>
#include <iostream>

#include "cost_model/cost_model_logical.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"
#include "optimizer/strategy/constant_calculation_rule.hpp"
#include "optimizer/strategy/exists_reformulation_rule.hpp"
#include "optimizer/strategy/in_reformulation_rule.hpp"
#include "optimizer/strategy/index_scan_rule.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "optimizer/strategy/logical_reduction_rule.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_db_generator.hpp"

using namespace opossum;  // NOLINT
//using namespace opossum::expression_functional;  // NOLINT

std::ofstream csv;

void execute_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  const auto before_translation = std::chrono::high_resolution_clock::now();

  auto pqp = LQPTranslator{}.translate_node(lqp);
  auto context = std::make_shared<TransactionContext>(0, 0);
  pqp->set_transaction_context_recursively(context);  //validate will fail if context is not set
  auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);

  const auto before_execution = std::chrono::high_resolution_clock::now();
  const auto elapsed_translation = std::chrono::duration<double>(before_execution - before_translation).count();

  for (auto& task : tasks) {
    task->schedule();
  }
  //  Print::print(tasks.back()->get_operator()->get_output());

  const auto after_execution = std::chrono::high_resolution_clock::now();
  const auto elapsed_execution = std::chrono::duration<double>(after_execution - before_execution).count();

  csv << elapsed_translation << ',' << elapsed_execution << '\n';
}

int main() {
  auto tpch_scale_factor = 1.0f;
  TpchDbGenerator{tpch_scale_factor}.generate_and_store();

  csv = std::ofstream("heuristic.csv", std::ios::app);
  csv << "optimization level,tpch scale factor,query name,elapsed pipeline,elapsed translation,elapsed execution\n";

  auto query_strings = std::vector{
      std::pair{"tpch_16", R"(
      SELECT p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
      FROM partsupp, part
      WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45'
        AND p_type not like 'MEDIUM POLISHED%'
        AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        AND ps_suppkey not in (
          SELECT s_suppkey
          FROM supplier
          WHERE s_comment LIKE '%Customer%Complaints%'
        )
      GROUP BY p_brand, p_type, p_size
      ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;
    )"},
  };

  //default optimizer without InReformulationRule
  auto optimizer = std::make_shared<Optimizer>(100);
  RuleBatch final_batch(RuleBatchExecutionPolicy::Once);
  final_batch.add_rule(std::make_shared<ConstantCalculationRule>());
  final_batch.add_rule(std::make_shared<LogicalReductionRule>());
  final_batch.add_rule(std::make_shared<ColumnPruningRule>());
  final_batch.add_rule(std::make_shared<ExistsReformulationRule>());
  //  final_batch.add_rule(std::make_shared<InReformulationRule>());
  final_batch.add_rule(std::make_shared<ChunkPruningRule>());
  final_batch.add_rule(std::make_shared<JoinOrderingRule>(std::make_shared<CostModelLogical>()));
  final_batch.add_rule(std::make_shared<PredicatePlacementRule>());
  final_batch.add_rule(std::make_shared<PredicateReorderingRule>());
  final_batch.add_rule(std::make_shared<IndexScanRule>());
  optimizer->add_rule_batch(final_batch);

  for (const auto& [query_name, query_string] : query_strings) {
    // not optimized at all
    {
      csv << "0," << tpch_scale_factor << ',' << query_name << ',';
      const auto before_pipeline = std::chrono::high_resolution_clock::now();

      auto sql_pipeline = SQLPipelineBuilder{query_string}.create_pipeline_statement();
      const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

      const auto after_pipeline = std::chrono::high_resolution_clock::now();
      const auto elapsed_pipeline = std::chrono::duration<double>(after_pipeline - before_pipeline).count();

      csv << elapsed_pipeline << ',';
      execute_lqp(lqp);
    }

    // optimized, without IN-reformulation
    {
      csv << "1," << tpch_scale_factor << ',' << query_name << ',';
      const auto before_pipeline = std::chrono::high_resolution_clock::now();

      auto sql_pipeline = SQLPipelineBuilder{query_string}.with_optimizer(optimizer).create_pipeline_statement();
      const auto& lqp = sql_pipeline.get_optimized_logical_plan();

      const auto after_pipeline = std::chrono::high_resolution_clock::now();
      const auto elapsed_pipeline = std::chrono::duration<double>(after_pipeline - before_pipeline).count();

      csv << elapsed_pipeline << ',';
      execute_lqp(lqp);
    }

    // optimized, with IN-reformulation
    {
      csv << "2," << tpch_scale_factor << ',' << query_name << ',';
      const auto before_pipeline = std::chrono::high_resolution_clock::now();

      auto sql_pipeline = SQLPipelineBuilder{query_string}.create_pipeline_statement();
      const auto& lqp = sql_pipeline.get_optimized_logical_plan();

      const auto after_pipeline = std::chrono::high_resolution_clock::now();
      const auto elapsed_pipeline = std::chrono::duration<double>(after_pipeline - before_pipeline).count();

      csv << elapsed_pipeline << ',';
      execute_lqp(lqp);
    }
  }
}
