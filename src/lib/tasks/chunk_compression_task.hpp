#pragma once

#include <memory>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace hyrise {

class Chunk;
class Table;

/**
 * @brief Compresses a chunk of a table using the default encoding
 *
 * The task compresses a chunk by sequentially compressing segments.
 * From each value segment, a dictionary segment is created that replaces the
 * uncompressed segment. The exchange is done atomically. Since this can
 * happen during simultaneous access by transactions, operators need to be
 * designed such that they are aware that segment types might change from
 * ValueSegment<T> to DictionarySegment<T> during execution. Shared pointers
 * ensure that existing value segments remain valid.
 *
 * Exchanging segments does not interfere with the Delete operator because
 * it does not touch the segments. However, inserting records while simultaneously
 * compressing the chunk leads to inconsistent state. Therefore only chunks where
 * all insertion has been completed may be compressed. In other words, they need to be
 * full and all of their end-cids must be smaller than infinity. This task calls
 * those chunks “completed”.
 *
 * Note: Reference segments are not invalidated by this task because the order in which
 *       records are stored does not change.
 */
class ChunkCompressionTask : public AbstractTask {
 public:
  explicit ChunkCompressionTask(const std::shared_ptr<Table>& table, const ChunkID chunk_id);
  explicit ChunkCompressionTask(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids);
  explicit ChunkCompressionTask(const std::shared_ptr<Table>& table, const ChunkID chunk_id,
                                const ChunkEncodingSpec& chunk_encoding_spec);
  explicit ChunkCompressionTask(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                const ChunkEncodingSpec& chunk_encoding_spec);

 protected:
  void _on_execute() override;

 private:
  const std::shared_ptr<Table> _table;
  const std::vector<ChunkID> _chunk_ids;
  std::optional<ChunkEncodingSpec> _chunk_encoding_spec;
};
}  // namespace hyrise
