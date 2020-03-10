#include "meta_log_table.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace opossum {

MetaLogTable::MetaLogTable()
    : AbstractMetaTable(TableColumnDefinitions{{"timestamp", DataType::Long, false},
                                               {"reporter", DataType::String, false},
                                               {"message", DataType::String, false}}) {}

const std::string& MetaLogTable::name() const {
  static const auto name = std::string{"log"};
  return name;
}

std::shared_ptr<Table> MetaLogTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& entry : Hyrise::get().log_manager.log_entries()) {
    output_table->append({static_cast<int64_t>(entry.timestamp.count()), pmr_string{entry.reporter}, pmr_string{entry.message}});
  }

  return output_table;
}


}  // namespace opossum
