#include "chunk_compression_task.hpp"

#include <string>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id)
    : ChunkCompressionTask{table_name, std::vector<ChunkID>{chunk_id}} {}

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids)
    : _table_name{table_name}, _chunk_ids{chunk_ids} {}

void ChunkCompressionTask::_on_execute() {
  auto table = StorageManager::get().get_table(_table_name);

  Assert(table != nullptr, "Table does not exist.");

  for (auto chunk_id : _chunk_ids) {
    Assert(chunk_id < table->chunk_count(), "Chunk with given ID does not exist.");

    auto chunk = table->get_chunk(chunk_id);

    DebugAssert(_chunk_is_completed(chunk, table->max_chunk_size()),
                "Chunk is not completed and thus can’t be compressed.");

    ChunkEncoder::encode_chunk(chunk, table->column_data_types());
  }
}

bool ChunkCompressionTask::_chunk_is_completed(const std::shared_ptr<Chunk>& chunk, const uint32_t max_chunk_size) {
  if (chunk->size() != max_chunk_size) return false;

  auto mvcc_columns = chunk->get_scoped_mvcc_columns_lock();

  for (const auto begin_cid : mvcc_columns->begin_cids) {
    if (begin_cid == MvccColumns::MAX_COMMIT_ID) return false;
  }

  return true;
}

}  // namespace opossum
