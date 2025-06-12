#include "chunk_compression_task.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id)
    : ChunkCompressionTask{table_name, std::vector<ChunkID>{chunk_id}} {}

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids)
    : _table_name{table_name}, _chunk_ids{chunk_ids} {}

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id,
                                           const ChunkEncodingSpec& chunk_encoding_spec)
    : _table_name(table_name), _chunk_ids{std::vector<ChunkID>{chunk_id}}, _chunk_encoding_spec(chunk_encoding_spec) {}

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                                           const ChunkEncodingSpec& chunk_encoding_spec)
    : _table_name(table_name), _chunk_ids{chunk_ids}, _chunk_encoding_spec(chunk_encoding_spec) {}

void ChunkCompressionTask::_on_execute() {
  const auto& table = Hyrise::get().storage_manager.get_table(_table_name);

  Assert(table, "Table does not exist.");

  auto chunk_encoding_spec = ChunkEncodingSpec{};
  if (_chunk_encoding_spec.has_value()) {
    chunk_encoding_spec = *_chunk_encoding_spec;
  } else {
    chunk_encoding_spec = auto_select_chunk_encoding_spec(table->column_data_types(), table->columns_are_nullable());
  }

  for (const auto chunk_id : _chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // TODO(anyone): It is unclear if this restriction is really necessary. If it becomes a problem and we decide to
    // get rid of it, we should make sure that a new mutable chunk is created first so that inserts do not end up in
    // the chunk being compressed.
    DebugAssert(_chunk_is_completed(chunk, table->target_chunk_size()),
                "Chunk is not completed and thus can’t be compressed.");

    ChunkEncoder::encode_chunk(chunk, table->column_data_types(), chunk_encoding_spec);
  }
}

bool ChunkCompressionTask::_chunk_is_completed(const std::shared_ptr<Chunk>& chunk, const uint32_t target_chunk_size) {
  if (chunk->size() != target_chunk_size) {
    return false;
  }

  const auto& mvcc_data = chunk->mvcc_data();

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < target_chunk_size; ++chunk_offset) {
    // TODO(anybody) Reading the non-atomic begin_cid (which is written to in Insert without a write lock) is likely UB
    //               When activating the ChunkCompressionTask, please look for a different means of determining whether
    //               all Inserts to a Chunk finished.
    if (mvcc_data->get_begin_cid(chunk_offset) == MAX_COMMIT_ID) {
      return false;
    }
  }

  return true;
}

}  // namespace hyrise
