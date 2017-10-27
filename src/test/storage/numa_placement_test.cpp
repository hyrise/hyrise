#if HYRISE_NUMA_SUPPORT

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/scheduler/current_scheduler.hpp"
#include "../lib/scheduler/node_queue_scheduler.hpp"
#include "../lib/scheduler/topology.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/dictionary_compression.hpp"
#include "../lib/storage/numa_placement_manager.hpp"
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/value_column.hpp"
#include "../lib/tasks/migration_preparation_task.hpp"
#include "../lib/types.hpp"

namespace opossum {

class NUMAPlacementTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto table = create_table(10, 1000);
    sm.add_table("table", table);
  }

  std::vector<size_t> count_chunks_by_node(const std::shared_ptr<Table>& table, size_t node_count) {
    std::vector<size_t> result(node_count);
    const auto chunk_count = table->chunk_count();
    for (ChunkID i = ChunkID(0); i < chunk_count; i++) {
      const auto& chunk = table->get_chunk(i);
      const auto node_id = MigrationPreparationTask::get_node_id(chunk.get_allocator());
      result.at(node_id)++;
    }
    return result;
  }

  std::shared_ptr<Table> create_table(size_t num_chunks, size_t num_rows_per_chunk) {
    auto table = std::make_shared<Table>(num_rows_per_chunk);
    table->add_column("a", "int", false);

    for (size_t i = 0; i < num_chunks; i++) {
      const auto alloc = PolymorphicAllocator<Chunk>(NUMAPlacementManager::get().get_memory_resource(0));
      auto chunk = Chunk(alloc, true, true);
      auto value_column = std::allocate_shared<ValueColumn<int>>(alloc, alloc);
      auto& values = value_column->values();
      values.reserve(num_rows_per_chunk);
      for (size_t row = 0; row < num_rows_per_chunk; row++) {
        values.push_back(static_cast<int>(row % 1000));
      }
      chunk.add_column(value_column);
      table->emplace_chunk(std::move(chunk));
    }
    DictionaryCompression::compress_table(*table);
    return table;
  }
};

TEST_F(NUMAPlacementTest, ChunkMigration) {
  const auto& topology = NUMAPlacementManager::get().topology();
  const auto& table = StorageManager::get().get_table("table");
  for (ChunkID i = ChunkID(0); i < table->chunk_count(); i++) {
    auto& chunk = table->get_chunk(i);
    for (size_t j = 0; j < 100; j++) {
      chunk.access_counter()->increment(100);
      chunk.access_counter()->process();
    }
  }

  EXPECT_EQ(count_chunks_by_node(table, topology->nodes().size())[0], size_t(10));

  auto options = NUMAPlacementManager::Options();
  options.counter_history_interval = std::chrono::seconds(1);
  options.counter_history_range = std::chrono::seconds(2);

  for (size_t i = 0; i < 2; i++) {
    MigrationPreparationTask(options).execute();
  }

  if (topology->nodes().size() > 1) {
    // At least one chunk has been migrated away from node 1.
    EXPECT_LT(count_chunks_by_node(table, topology->nodes().size())[0], size_t(10));
  }
}

}  // namespace opossum

#endif
