#if HYRISE_NUMA_SUPPORT

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/scheduler/current_scheduler.hpp"
#include "../lib/scheduler/node_queue_scheduler.hpp"
#include "../lib/scheduler/topology.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/chunk_encoder.hpp"
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
    NUMAPlacementManager::get().resume();

    const auto table = create_table(_chunk_count, 1000);
    StorageManager::get().add_table("table", table);

    _node_count = NUMAPlacementManager::get().topology()->nodes().size();
  }

  // Returns a vector that contains the counts of chunks per node.
  // The index of the vector represents the NodeID.
  std::vector<size_t> count_chunks_by_node(const std::shared_ptr<Table>& table) {
    std::vector<size_t> result(_node_count);
    const auto chunk_count = table->chunk_count();
    for (ChunkID i = ChunkID(0); i < chunk_count; i++) {
      const auto chunk = table->get_chunk(i);
      const auto node_id = MigrationPreparationTask::get_node_id(chunk->get_allocator());
      result.at(node_id)++;
    }
    return result;
  }

  // Creates a table with a single column and increasing integers modulo 1000.
  std::shared_ptr<Table> create_table(size_t num_chunks, size_t num_rows_per_chunk) {
    auto table = std::make_shared<Table>(num_rows_per_chunk);
    table->add_column("a", DataType::Int, false);

    for (size_t i = 0; i < num_chunks; i++) {
      const auto alloc = PolymorphicAllocator<Chunk>(NUMAPlacementManager::get().get_memory_resource(0));
      auto chunk = std::make_shared<Chunk>(alloc, UseMvcc::Yes, ChunkUseAccessCounter::Yes);
      auto value_column = std::allocate_shared<ValueColumn<int>>(alloc, alloc);
      auto& values = value_column->values();
      values.reserve(num_rows_per_chunk);
      for (size_t row = 0; row < num_rows_per_chunk; row++) {
        values.push_back(static_cast<int>(row % 1000));
      }
      chunk->add_column(value_column);
      table->emplace_chunk(std::move(chunk));
    }
    ChunkEncoder::encode_all_chunks(table);
    return table;
  }

  size_t _node_count;
  static constexpr size_t _chunk_count = 10;
};

// Tests the chunk migration algorithm without the integrated loop
// of NUMAPlacementManager.
TEST_F(NUMAPlacementTest, ChunkMigration) {
  const auto& table = StorageManager::get().get_table("table");
  const auto& options = NUMAPlacementManager::get().options();

  // Set mocked chunk access times
  for (ChunkID i = ChunkID(0); i < table->chunk_count(); i++) {
    auto chunk = table->get_chunk(i);
    for (size_t j = 0; j < 100; j++) {
      chunk->access_counter()->increment(100);
      chunk->access_counter()->process();
    }
  }

  // Initially all chunks should reside on node 0
  EXPECT_EQ(count_chunks_by_node(table)[0], _chunk_count);

  // Run two migrations
  for (size_t i = 0; i < 2; i++) {
    MigrationPreparationTask(options).execute();
  }

  if (_node_count > 1) {
    // At least one chunk has been migrated away from node 0.
    EXPECT_LT(count_chunks_by_node(table)[0], _chunk_count);
  }
}

// Tests the integrated loop of NUMAPlacementManager.
TEST_F(NUMAPlacementTest, IntegratedLoopTest) {
  const auto& table = StorageManager::get().get_table("table");
  const auto& options = NUMAPlacementManager::get().options();

  // Initially all chunks should reside on node 0
  EXPECT_EQ(count_chunks_by_node(table)[0], _chunk_count);

  // Start the loop
  NUMAPlacementManager::get().resume();

  // Simulate chunk accesses, ChunkMetricsCollectionTask should pick
  // those up
  for (size_t j = 0; j < 150; j++) {
    for (ChunkID i = ChunkID(0); i < table->chunk_count(); i++) {
      auto chunk = table->get_chunk(i);
      chunk->access_counter()->increment(100);
    }
    std::this_thread::sleep_for(options.counter_history_interval);
  }

  // MigrationPreparationTask and chunk migration should have run at least once by now

  if (_node_count > 1) {
    // At least one chunk has been migrated away from node 1.
    EXPECT_LT(count_chunks_by_node(table)[0], _chunk_count);
  }

  // Stop the loop
  NUMAPlacementManager::get().pause();
}

}  // namespace opossum

#endif
