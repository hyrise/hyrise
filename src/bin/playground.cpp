#include <iostream>
#include <sql/sql_pipeline_builder.hpp>

#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::cout << "Hello world!!" << std::endl;

  const auto query = tpch_queries.at(21);

  const auto tables = TpchDbGenerator(0.01f, 1000).generate();

  for (auto& tpch_table : tables) {
    const auto& table_name = tpch_table_names.at(tpch_table.first);
    const auto& table = tpch_table.second;
    StorageManager::get().add_table(table_name, table);

    std::cout << "Encoded table " << table_name << " successfully." << std::endl;
  }

  std::cout << query << std::endl;

  const auto pipeline_builder = SQLPipelineBuilder{query};
  auto pipeline = pipeline_builder.create_pipeline();
  auto optimized = pipeline.get_optimized_logical_plans();
  for (const auto& plan : optimized) {
    plan->print();
    std::cout << std::endl;
  }

  return 0;
}
