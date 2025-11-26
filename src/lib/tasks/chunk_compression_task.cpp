#include "chunk_compression_task.hpp"

#include <memory>
#include <vector>

#include "storage/chunk_encoder.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/encoding_type.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

ChunkCompressionTask::ChunkCompressionTask(const std::shared_ptr<Table>& table, const ChunkID chunk_id)
    : ChunkCompressionTask{table, std::vector<ChunkID>{chunk_id}} {}

ChunkCompressionTask::ChunkCompressionTask(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids)
    : _table{table}, _chunk_ids{chunk_ids} {}

ChunkCompressionTask::ChunkCompressionTask(const std::shared_ptr<Table>& table, const ChunkID chunk_id,
                                           const ChunkEncodingSpec& chunk_encoding_spec)
    : _table(table), _chunk_ids{std::vector<ChunkID>{chunk_id}}, _chunk_encoding_spec(chunk_encoding_spec) {}

ChunkCompressionTask::ChunkCompressionTask(const std::shared_ptr<Table>& table, const std::vector<ChunkID>& chunk_ids,
                                           const ChunkEncodingSpec& chunk_encoding_spec)
    : _table(table), _chunk_ids{chunk_ids}, _chunk_encoding_spec(chunk_encoding_spec) {}

void ChunkCompressionTask::_on_execute() {
  Assert(_table, "Table does not exist.");

  const auto default_encoding_spec =
      auto_select_chunk_encoding_spec(_table->column_data_types(), unique_columns(_table));
  const auto chunk_encoding_spec = _chunk_encoding_spec.value_or(default_encoding_spec);

  for (const auto chunk_id : _chunk_ids) {
    Assert(chunk_id < _table->chunk_count(), "Chunk with given ID does not exist.");
    const auto chunk = _table->get_chunk(chunk_id);
    if (!chunk) {
      // We access the table directly (not via a GetTable operator). Thus, the chunk might have been deleted.
      continue;
    }
    Assert(!chunk->is_mutable(), "Mutable chunks cannot be compressed.");

    ChunkEncoder::encode_chunk(chunk, _table->column_data_types(), chunk_encoding_spec);
  }
}

}  // namespace hyrise
