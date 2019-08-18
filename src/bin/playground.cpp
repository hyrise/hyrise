#include <iostream>
#include <sstream>

#include "optimizer/strategy/in_expression_to_join_rule.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "visualization/pqp_visualizer.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto scale_factor = .01f;
  TPCHTableGenerator{scale_factor, Chunk::DEFAULT_SIZE}.generate_and_store();

  std::cout << "algorithm,list_length,execution_duration" << std::endl;
  
  const auto run = [](const std::string& name, const InExpressionToJoinRule::Algorithm algorithm){
    InExpressionToJoinRule::forced_algorithm = algorithm;

    for (auto list_length = 1; list_length < 10; ++list_length) {
      auto sql_stream = std::stringstream{};
      sql_stream << "SELECT * FROM lineitem WHERE l_partkey IN (";
      for (auto list_item_idx = 0; list_item_idx < list_length; ++list_item_idx) {
        sql_stream << list_item_idx;
        if (list_item_idx < list_length - 1) sql_stream << ", ";
      }
      sql_stream << ");";

      auto pipeline = SQLPipelineBuilder{sql_stream.str()}.dont_cleanup_temporaries().create_pipeline();
      pipeline.get_result_table();
      std::cout << name << "," << pipeline.metrics().statement_metrics[0]->plan_execution_duration.count() << std::endl;

      PQPVisualizer{}.visualize(pipeline.get_physical_plans(), name + std::to_string(list_length) + ".png");
    }
  };

  run("ExpressionEvaluator", InExpressionToJoinRule::Algorithm::ExpressionEvaluator);
  run("Join", InExpressionToJoinRule::Algorithm::Join);
  run("Disjunction", InExpressionToJoinRule::Algorithm::Disjunction);
  run("Auto", InExpressionToJoinRule::Algorithm::Auto);

  return 0;
}
