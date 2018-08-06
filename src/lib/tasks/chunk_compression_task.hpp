#pragma once

#include <string>
#include <vector>

#include "scheduler/abstract_task.hpp"

namespace opossum {

class Chunk;

/**
 * @brief Compresses a chunk of a table using the default encoding
 *
 * The task compresses a chunk by sequentially compressing columns.
 * From each value column, a dictionary column is created that replaces the
 * uncompressed column. The exchange is done atomically. Since this can
 * happen during simultaneous access by transactions, operators need to be
 * designed such that they are aware that column types might change from
 * ValueColumn<T> to DictionaryColumn<T> during execution. Shared pointers
 * ensure that existing value columns remain valid.
 *
 * Exchanging columns does not interfere with the Delete operator because
 * it does not touch the columns. However, inserting records while simultaneously
 * compressing the chunk leads to inconsistent state. Therefore only chunks where
 * all insertion has been completed may be compressed. In other words, they need to be
 * full and all of their end-cids must be smaller than infinity. This task calls
 * those chunks “completed”.
 *
 * Note: Reference columns are not invalidated by this task because the order in which
 *       records are stored does not change.
 */
class ChunkCompressionTask : public AbstractTask {
 public:
  explicit ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id);
  explicit ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids);

 protected:
  void _on_execute() override;

 private:
  /**
   * @brief Checks if a chunks is completed
   *
   * See class comment for further explanation
   */
  bool _chunk_is_completed(const std::shared_ptr<Chunk>& chunk, const uint32_t max_chunk_size);

 private:
  const std::string _table_name;
  const std::vector<ChunkID> _chunk_ids;
};
}  // namespace opossum
