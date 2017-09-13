#pragma once

#include <string>
#include <vector>

#include "scheduler/abstract_task.hpp"

namespace opossum {

class Chunk;

/**
 * @brief Compresses a chunk of a table
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
 * After the columns have been replaced, the task calls Chunk::shrink_mvcc_columns()
 * in order to reduce fragmentation of the MVCC columns. The MVCC columns are locked
 * exclusively during this step.
 *
 * Note: Reference columns are not invalidated by this task because the order in which
 *       records are stored does not change.
 */
class ChunkCompressionTask : public AbstractTask {
 public:
  explicit ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id, bool check_completion = true);
  explicit ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                                bool check_completion = true);

 protected:
  void _on_execute() override;

 private:
  /**
   * @brief Checks if a chunks is completed
   *
   * See class comment for further explanation
   */
  bool chunk_is_completed(const Chunk& chunk, const uint32_t max_chunk_size);

 private:
  const bool _check_completion;  ///< decides whether chunk_is_completed is called before compression
  const std::string _table_name;
  const std::vector<ChunkID> _chunk_ids;
};
}  // namespace opossum
