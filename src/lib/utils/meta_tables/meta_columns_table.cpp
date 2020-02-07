#include "meta_columns_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaColumnsTable::MetaColumnsTable()
    : _column_definitions(TableColumnDefinitions{{"table_name", DataType::String, false},
                                                 {"column_count", DataType::Int, false},
                                                 {"row_count", DataType::Long, false},
                                                 {"chunk_count", DataType::Int, false},
                                                 {"max_chunk_size", DataType::Long, false}}) {}

const std::string& MetaColumnsTable::name() const {
  static const auto name = std::string{"columns"};
  return name;
}

std::shared_ptr<Table> MetaColumnsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      output_table->append({pmr_string{table_name}, static_cast<pmr_string>(table->column_name(column_id)),
                            static_cast<pmr_string>(data_type_to_string.left.at(table->column_data_type(column_id))),
                            static_cast<int32_t>(table->column_is_nullable(column_id))});
    }
  }

  return output_table;
}

}  // namespace opossum
