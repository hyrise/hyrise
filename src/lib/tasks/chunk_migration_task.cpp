#include "chunk_migration_task.hpp"

#if HYRISE_NUMA_SUPPORT

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "storage/numa_placement_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

ChunkMigrationTask::ChunkMigrationTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                                       int target_node_id)
    : _table_name(table_name), _target_node_id(target_node_id), _chunk_ids(chunk_ids) {}

void ChunkMigrationTask::_on_execute() {
  auto table = StorageManager::get().get_table(_table_name);

  if (!table) {
    throw std::logic_error("Table does not exist.");
  }

  for (auto chunk_id : _chunk_ids) {
    DebugAssert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");

    auto& chunk = table->get_chunk(chunk_id);

    // TODO(normanrz): Comment Why?
    DebugAssert(chunk_is_completed(chunk, table->chunk_size()), "Chunk is not completed and thus can’t be migrated.");

    // TODO(normanrz): Remove debug output
    std::cout << "Starting migration " << _table_name << " " << chunk_id << std::endl;
    chunk.migrate(NUMAPlacementManager::get()->get_memory_resource(_target_node_id));
    std::cout << "Completed migration " << _table_name << " " << chunk_id << std::endl;
  }
}

bool ChunkMigrationTask::chunk_is_completed(const Chunk& chunk, const uint32_t max_chunk_size) {
  if (chunk.size() != max_chunk_size) return false;

  if (chunk.has_mvcc_columns()) {
    auto mvcc_columns = chunk.mvcc_columns();

    for (const auto begin_cid : mvcc_columns->begin_cids) {
      if (begin_cid == Chunk::MAX_COMMIT_ID) return false;
    }
  }

  return true;
}

}  // namespace opossum

#endif
