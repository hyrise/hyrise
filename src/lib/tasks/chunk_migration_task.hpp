#pragma once

#if HYRISE_NUMA_SUPPORT

#include <string>
#include <vector>

#include "scheduler/abstract_task.hpp"

namespace opossum {

class Chunk;

class ChunkMigrationTask : public AbstractTask {
 public:
  explicit ChunkMigrationTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids, int target_node_id);
  static bool chunk_is_completed(const std::shared_ptr<const Chunk>& chunk, const uint32_t max_chunk_size);

 protected:
  void _on_execute() override;

 private:
  const std::string _table_name;
  const int _target_node_id;
  const std::vector<ChunkID> _chunk_ids;
};
}  // namespace opossum

#endif
