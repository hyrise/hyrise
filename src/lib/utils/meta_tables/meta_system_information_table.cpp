#include "meta_system_information_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaSystemInformationTable::MetaSystemInformationTable()
    : AbstractMetaTable(TableColumnDefinitions{{"cpu_count", DataType::Int, false}, {"ram", DataType::Int, false}}) {}

const std::string& MetaSystemInformationTable::name() const {
  static const auto name = std::string{"system_information"};
  return name;
}

std::shared_ptr<Table> MetaSystemInformationTable::_on_generate() {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  // TODO(j-tr): generate actual table with static system information

  return output_table;
}

}  // namespace opossum
