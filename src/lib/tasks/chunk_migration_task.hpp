#pragma once

#include <string>
#include <vector>

#include "scheduler/abstract_task.hpp"

namespace opossum {

class Chunk;

class ChunkMigrationTask : public AbstractTask {
 public:
  explicit ChunkMigrationTask(const std::string& table_name, const ChunkID chunk_id, int node_id);
  explicit ChunkMigrationTask(const std::string& table_name, const alloc_vector<ChunkID>& chunk_ids, int node_id);

 protected:
  void on_execute() override;

 private:
  /**
   * @brief Checks if a chunks is completed
   *
   * See class comment for further explanation
   */
  bool chunk_is_completed(const Chunk& chunk, const uint32_t max_chunk_size);

 private:
  const std::string _table_name;
  const int _node_id;
  const alloc_vector<ChunkID> _chunk_ids;
};
}  // namespace opossum
