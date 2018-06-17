#include <iostream>

#include "types.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"
#include "tpch/tpch_queries.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "planviz/lqp_visualizer.hpp"

using namespace opossum;  // NOLINT
using namespace std::string_literals;  // NOLINT

int main() {
  // Chosen rather arbitrarily
  const auto chunk_size = 1'000;

  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";

  std::vector<std::string> tpch_table_names(
  {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"});

  for (const auto& tpch_table_name : tpch_table_names) {
    const auto tpch_table_path = std::string("src/test/tables/tpch/sf-0.001/") + tpch_table_name + ".tbl";
    StorageManager::get().add_table(tpch_table_name, load_table(tpch_table_path, chunk_size));
  }

  for (const auto& pair : tpch_queries) {
    if (pair.first != 16) continue;

    try {
      auto pipeline = SQLPipelineBuilder{pair.second}.create_pipeline();

      size_t idx = 0;
      for (const auto &query_plan : pipeline.get_query_plans()) {
        SQLQueryPlanVisualizer{graphviz_config, {}, {}, {}}.visualize(*query_plan, "tmp.dot",
                                                                      "pqp_tpch-" + std::to_string(pair.first) + "-" +
                                                                      std::to_string(idx) + ".svg");
        ++idx;
      }

      idx = 0;
      for (const auto &lqp : pipeline.get_optimized_logical_plans()) {
        LQPVisualizer{graphviz_config, {}, {}, {}}.visualize({lqp}, "tmp.dot",
                                                                      "lqp_tpch-" + std::to_string(pair.first) + "-" +
                                                                      std::to_string(idx) + ".svg");
        ++idx;
      }
    } catch(const std::exception& e) {
      std::cout << "Failed to visualize " << pair.first << ": " << e.what() << std::endl;
    }
  }

  return 0;
}
