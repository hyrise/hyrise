#include "chunk_migration_task.hpp"

#include <memory>
#include <string>
#include <vector>

#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#if HYRISE_NUMA_SUPPORT

namespace opossum {

ChunkMigrationTask::ChunkMigrationTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                                       int target_node_id, SchedulePriority priority, bool stealable)
    : AbstractTask(priority, stealable),
      _table_name(table_name),
      _target_node_id(target_node_id),
      _chunk_ids(chunk_ids) {}

void ChunkMigrationTask::_on_execute() {
  auto table = StorageManager::get().get_table(_table_name);

  if (!table) {
    throw std::logic_error("Table does not exist.");
  }

  for (auto chunk_id : _chunk_ids) {
    DebugAssert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");

    auto chunk = table->get_chunk(chunk_id);

    // Only completed chunks are supported for migration, because they
    // are largely immutable. Currently there is no concurrency control
    // in place that would allow the safe migration of mutable chunks.
    DebugAssert(chunk_is_completed(chunk, table->max_chunk_size()),
                "Chunk is not completed and thus can’t be migrated.");

    chunk->migrate(Topology::get().get_memory_resource(_target_node_id));
  }
}

bool ChunkMigrationTask::chunk_is_completed(const std::shared_ptr<const Chunk>& chunk, const uint32_t max_chunk_size) {
  if (chunk->size() != max_chunk_size) return false;

  if (chunk->has_mvcc_columns()) {
    auto mvcc_columns = chunk->get_scoped_mvcc_columns_lock();

    for (const auto begin_cid : mvcc_columns->begin_cids) {
      if (begin_cid == MvccColumns::MAX_COMMIT_ID) return false;
    }
  }

  return true;
}

}  // namespace opossum

#else
int chunk_migration_task_dummy;
#endif
