#include <chrono>
#include <cstdlib>
#include <iostream>

#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto num_rows = size_t{1000};
  const auto num_chunks = size_t{50};
  const auto num_colums = size_t{1};
  const auto chunk_size = static_cast<ChunkID>(num_rows / num_chunks);  // 100.000
  const auto multiple_nodes = false;
  const auto encoding = EncodingType::Dictionary;

  auto config = ColumnDataDistribution::make_uniform_config(0.0, 10000);
  auto configs = std::vector<ColumnDataDistribution>();
  for (auto column_count = size_t{0}; column_count < num_colums ; ++column_count) {
    configs.push_back(config);
  }

  std::cout << "Generating tables ..." << std::endl;

  auto table_generator = std::make_shared<TableGenerator>();
  auto table_1 = table_generator->generate_table(configs, num_rows, chunk_size, encoding, multiple_nodes);
  auto table_2 = table_generator->generate_table(configs, num_rows, chunk_size, encoding, multiple_nodes);

  std::cout << "Generating tables done" << std::endl;

  CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>(NUMAPlacementManager::get().topology()));

  auto table_wrapper_1 = std::make_shared<TableWrapper>(table_1);
  auto table_wrapper_2 = std::make_shared<TableWrapper>(table_2);
  table_wrapper_1->execute();
  table_wrapper_2->execute();

  auto join = std::make_shared<JoinSortMerge>(table_wrapper_1, table_wrapper_2, JoinMode::Inner, std::pair<ColumnID, ColumnID>{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  std::cout << "Executing SortMergeJoin ..." << std::endl;
  join->execute();
  std::cout << "Executing SortMergeJoin done" << std::endl;

  return 0;
}
