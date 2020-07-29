#include "meta_chunk_sort_orders_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaChunkSortOrdersTable::MetaChunkSortOrdersTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"chunk_id", DataType::Int, false},
                                               {"column_id", DataType::Int, false},
                                               {"order_mode", DataType::String, false}}) {}

const std::string& MetaChunkSortOrdersTable::name() const {
  static const auto name = std::string{"chunk_sort_orders"};
  return name;
}

/**
 * Chunks can be sorted independently by multiple columns (e.g., as the result of an equi sort-merge join or
 * accidentally). We do not track secondary sort orders, meaning that for `ORDER BY a, b`, only a is reported.
 */
std::shared_ptr<Table> MetaChunkSortOrdersTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (!chunk) continue;  // Skip physically deleted chunks

      const auto& sorted_by = chunk->individually_sorted_by();
      if (!sorted_by.empty()) {
        for (const auto& [sorted_by_column_id, sort_mode] : sorted_by) {
          std::stringstream sort_mode_stream;
          sort_mode_stream << sort_mode;
          output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id),
                                static_cast<int32_t>(sorted_by_column_id), pmr_string{sort_mode_stream.str()}});
        }
      }
    }
  }

  return output_table;
}

}  // namespace opossum
