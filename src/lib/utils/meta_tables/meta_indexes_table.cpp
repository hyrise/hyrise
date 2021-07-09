#include "meta_indexes_table.hpp"
#include "storage/index/segment_index_type.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaIndexesTable::MetaIndexesTable()
    : AbstractMetaTable(TableColumnDefinitions{{"index_name", DataType::String, false},
                                               {"index_type", DataType::String, false},
                                               {"table_name", DataType::String, false},
                                               {"column_names", DataType::String, false},
                                               }) {}

const std::string& MetaIndexesTable::name() const {
  static const auto name = std::string{"indexes"};
  return name;
}

std::shared_ptr<Table> MetaIndexesTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    for (auto index : table->indexes_statistics()) {
      std::string column_names ("");
      for (auto col_id : index.column_ids) {
        column_names += " " + table->column_name(col_id);
      }
      // TODO: convert index.type to string instead of using "GroupKeyIndex"
      output_table->append({pmr_string{index.name}, "GroupKeyIndex",
                            static_cast<pmr_string>(table_name),
                            static_cast<pmr_string>(column_names),
                            });
    }
  }
  return output_table;
}

}  // namespace opossum
