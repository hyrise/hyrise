#include "meta_chunks_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaChunksTable::MetaChunksTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"chunk_id", DataType::Int, false},
                                               {"row_count", DataType::Long, false},
                                               {"invalid_row_count", DataType::Long, false},
                                               {"cleanup_commit_id", DataType::Long, true}}) {}

const std::string& MetaChunksTable::name() const {
  static const auto name = std::string{"chunks"};
  return name;
}

std::shared_ptr<Table> MetaChunksTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      const auto cleanup_commit_id = chunk->get_cleanup_commit_id()
                                         ? AllTypeVariant{static_cast<int64_t>(*chunk->get_cleanup_commit_id())}
                                         : NULL_VALUE;
      output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id), static_cast<int64_t>(chunk->size()),
                            static_cast<int64_t>(chunk->invalid_row_count()), cleanup_commit_id});
    }
  }

  return output_table;
}

}  // namespace opossum
