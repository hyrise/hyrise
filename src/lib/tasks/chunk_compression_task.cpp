#include "chunk_compression_task.hpp"

#include <memory>
#include <string>
#include <vector>

#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const ChunkID chunk_id, bool check_completion)
    : ChunkCompressionTask{table_name, std::vector<ChunkID>{chunk_id}, check_completion} {}

ChunkCompressionTask::ChunkCompressionTask(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                                           bool check_completion)
    : _check_completion{check_completion}, _table_name{table_name}, _chunk_ids{chunk_ids} {}

void ChunkCompressionTask::on_execute() {
  auto table = StorageManager::get().get_table(_table_name);

  if (!table) {
    throw std::logic_error("Table does not exist.");
  }

  for (auto chunk_id : _chunk_ids) {
    if (chunk_id >= table->chunk_count()) {
      throw std::logic_error("Chunk with given ID does not exist.");
    }

    auto& chunk = table->get_chunk(chunk_id);

    if (_check_completion && !chunk_is_completed(chunk, table->chunk_size())) {
      throw std::logic_error("Chunk is not completed and thus can’t be compressed.");
    }

    DictionaryCompression::compress_chunk(table->column_types(), chunk);
  }
}

bool ChunkCompressionTask::chunk_is_completed(const Chunk& chunk, const uint32_t max_chunk_size) {
  if (chunk.size() != max_chunk_size) return false;

  auto mvcc_columns = chunk.mvcc_columns();

  for (const auto begin_cid : mvcc_columns->begin_cids) {
    if (begin_cid == Chunk::MAX_COMMIT_ID) return false;
  }

  return true;
}

}  // namespace opossum
