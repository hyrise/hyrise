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
 * At the moment, each chunk can be sorted by exactly one column or none. Hence, having a column within the chunk table
 * would be sufficient. However, this will change in the near future (e.g., when a sort-merge join evicts a chunk that
 * is sorted on two columns). To prepare for this change, this additional table stores the sort orders and allows a
 * chunk to have multiple sort orders. Cascading sort orders for chunks are currently not planned.
 */
std::shared_ptr<Table> MetaChunkSortOrdersTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto& chunk = table->get_chunk(chunk_id);
      const auto ordered_by = chunk->ordered_by();
      if (ordered_by) {
        std::stringstream order_by_mode_steam;
        order_by_mode_steam << ordered_by->second;
        output_table->append({pmr_string{table_name}, static_cast<int32_t>(chunk_id),
                              static_cast<int32_t>(ordered_by->first), pmr_string{order_by_mode_steam.str()}});
      }
    }
  }

  return output_table;
}

}  // namespace opossum
