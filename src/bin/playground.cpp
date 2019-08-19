#include <iostream>
#include <sstream>

#include "../benchmarklib/benchmark_config.hpp"
#include "optimizer/strategy/in_expression_to_join_rule.hpp"
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

  std::cout << "algorithm,list_length,execution_duration" << std::endl;
  
  const auto run = [](const std::string& name, const InExpressionToJoinRule::Algorithm algorithm){
    InExpressionToJoinRule::forced_algorithm = algorithm;

    auto warmup = true;

    for (auto list_length : std::vector<int>{1, 2, 3, 4, 5, 6, 8, 10, 15, 20, 25, 30, 50, 75, 100, 150, 200, 300, 400}) {
      start:
      auto sql_stream = std::stringstream{};
      // Don't choose an ID column here as that would allow the scan to prune most chunks
      sql_stream << "SELECT * FROM lineitem WHERE l_suppkey IN (";
      for (auto list_item_idx = 0; list_item_idx < list_length; ++list_item_idx) {
        sql_stream << list_item_idx;
        if (list_item_idx < list_length - 1) sql_stream << ", ";
      }
      sql_stream << ");";

      auto pipeline = SQLPipelineBuilder{sql_stream.str()}.dont_cleanup_temporaries().create_pipeline();
      pipeline.get_result_table();

      if (warmup) {
        // Throw away result, restart
        warmup = false;
        goto start;
      }

      std::cout << name << "," << list_length << "," << pipeline.metrics().statement_metrics[0]->plan_execution_duration.count() << std::endl;

      // PQPVisualizer{}.visualize(pipeline.get_physical_plans(), name + std::to_string(list_length) + ".png");
    }
  };

  run("ExpressionEvaluator", InExpressionToJoinRule::Algorithm::ExpressionEvaluator);  // TODO will be faster with mrks/rundumschlag
  run("Join", InExpressionToJoinRule::Algorithm::Join);
  run("Disjunction", InExpressionToJoinRule::Algorithm::Disjunction);
  // run("Auto", InExpressionToJoinRule::Algorithm::Auto);

  return 0;
}
