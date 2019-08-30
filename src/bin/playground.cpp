#include <iostream>
#include <sstream>

#include "../benchmarklib/benchmark_config.hpp"
#include "optimizer/strategy/in_expression_rewrite_rule.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "visualization/pqp_visualizer.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto scale_factor = 1.f;
  auto benchmark_config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  benchmark_config->encoding_config = EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}};
  TPCHTableGenerator{scale_factor, benchmark_config}.generate_and_store();

  // const auto scheduler = std::make_shared<NodeQueueScheduler>();
  // CurrentScheduler::set(scheduler);

  std::cout << "algorithm,list_length,execution_duration,rows" << std::endl;

  const auto run = [](const std::string& name, const InExpressionRewriteRule::Algorithm algorithm) {
    InExpressionRewriteRule::forced_algorithm = algorithm;

    auto warmup = true;

    for (auto list_length = 1; list_length < 100; ++list_length) {
    start:
      auto sql_stream = std::stringstream{};
      // Don't choose an ID column here as that would allow the scan to prune most chunks
      sql_stream << "SELECT * FROM lineitem WHERE l_suppkey IN (";
      for (auto list_item_idx = 0; list_item_idx <= list_length; ++list_item_idx) {
        sql_stream << list_item_idx + 1;
        if (list_item_idx < list_length - 1) sql_stream << ", ";
      }
      sql_stream << ");";

      auto pipeline = SQLPipelineBuilder{sql_stream.str()}.dont_cleanup_temporaries().create_pipeline();
      auto result_table = pipeline.get_result_table();

      if (warmup) {
        // Throw away result, restart
        warmup = false;
        goto start;
      }

      std::cout << name << "," << list_length << ","
                << pipeline.metrics().statement_metrics[0]->plan_execution_duration.count() << "," << result_table.second->row_count() << std::endl;

      // PQPVisualizer{}.visualize(pipeline.get_physical_plans(), name + std::to_string(list_length) + ".png");
    }
  };

  run("ExpressionEvaluator", InExpressionRewriteRule::Algorithm::ExpressionEvaluator);
  run("Join", InExpressionRewriteRule::Algorithm::Join);
  run("Auto", InExpressionRewriteRule::Algorithm::Auto);
  run("Disjunction", InExpressionRewriteRule::Algorithm::Disjunction);

  return 0;
}

