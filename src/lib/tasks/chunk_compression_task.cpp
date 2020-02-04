#include "chunk_compression_task.hpp"

#include <string>
#include <vector>

#include "hyrise.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id)
    : ChunkCompressionTask{table_name, std::vector<ChunkID>{chunk_id}} {}

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids)
    : _table_name{table_name}, _chunk_ids{chunk_ids} {}

void ChunkCompressionTask::_on_execute() {
  auto table = Hyrise::get().storage_manager.get_table(_table_name);

  Assert(table, "Table does not exist.");

  for (auto chunk_id : _chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");
    const auto chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // TODO(anyone): It is unclear if this restriction is really necessary. If it becomes a problem and we decide to
    // get rid of it, we should make sure that a new mutable chunk is created first so that inserts do not end up in
    // the chunk being compressed.
    DebugAssert(_chunk_is_completed(chunk, table->target_chunk_size()),
                "Chunk is not completed and thus can’t be compressed.");

    ChunkEncoder::encode_chunk(chunk, table->column_data_types());
  }
}

bool ChunkCompressionTask::_chunk_is_completed(const std::shared_ptr<Chunk>& chunk, const uint32_t target_chunk_size) {
  if (chunk->size() != target_chunk_size) return false;

  const auto& mvcc_data = chunk->mvcc_data();

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < target_chunk_size; ++chunk_offset) {
    // TODO(anybody) Reading the non-atomic begin_cid (which is written to in Insert without a write lock) is likely UB
    //               When activating the ChunkCompressionTask, please look for a different means of determining whether
    //               all Inserts to a Chunk finished.
    if (mvcc_data->get_begin_cid(chunk_offset) == MvccData::MAX_COMMIT_ID) return false;
  }

  return true;
}

}  // namespace opossum
