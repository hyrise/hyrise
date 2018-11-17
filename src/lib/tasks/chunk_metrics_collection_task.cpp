#include "chunk_metrics_collection_task.hpp"

#if HYRISE_NUMA_SUPPORT

#include <memory>
#include <string>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

void ChunkMetricsCollectionTask::_on_execute() {
  for (const auto& table_pair : StorageManager::get().tables()) {
    const auto& table = table_pair.second;
    for (ChunkID i = ChunkID(0); i < table->chunk_count(); i++) {
      const auto chunk = table->get_chunk(i);
      if (const auto access_counter = chunk->access_counter()) {
        access_counter->process();
      }
    }
  }
}

}  // namespace opossum

#else
int chunk_metrics_collection_task_dummy;
#endif
