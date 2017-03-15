#include "chunk_compression.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

ChunkCompression::ChunkCompression(const std::string& table_name, const ChunkID chunk_id)
    : _table_name{table_name}, _chunk_id{chunk_id} {}

const std::string ChunkCompression::name() const { return "ChunkCompression"; }
uint8_t ChunkCompression::num_in_tables() const { return 0u; }
uint8_t ChunkCompression::num_out_tables() const { return 0u; }

std::shared_ptr<const Table> ChunkCompression::on_execute(TransactionContext* context) {
  auto table = StorageManager::get().get_table(_table_name);

  if (!table) {
    throw std::logic_error("Table does not exist.");
  }

  if (_chunk_id >= table->chunk_count()) {
    throw std::logic_error("Chunk with given ID does not exist.");
  }

  auto& chunk = table->get_chunk(_chunk_id);

  for (auto column_id = 0u; column_id < chunk.size(); ++column_id) {
    auto column = chunk.get_column(column_id);
    auto dict_column = make_shared_by_column_type<BaseColumn, DictionaryColumn>(table->column_type(column_id), column);
    chunk.set_column(column_id, dict_column);
  }

  return nullptr;
}

}  // namespace opossum
