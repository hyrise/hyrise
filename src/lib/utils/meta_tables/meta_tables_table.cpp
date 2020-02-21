#include "meta_tables_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaTablesTable::MetaTablesTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"column_count", DataType::Int, false},
                                               {"row_count", DataType::Long, false},
                                               {"chunk_count", DataType::Int, false},
                                               {"target_chunk_size", DataType::Long, false}}) {}

const std::string& MetaTablesTable::name() const {
  static const auto name = std::string{"tables"};
  return name;
}

std::shared_ptr<Table> MetaTablesTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    output_table->append({pmr_string{table_name}, static_cast<int32_t>(table->column_count()),
                          static_cast<int64_t>(table->row_count()), static_cast<int32_t>(table->chunk_count()),
                          static_cast<int64_t>(table->target_chunk_size())});
  }

  return output_table;
}

}  // namespace opossum
