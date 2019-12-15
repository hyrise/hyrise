#include "drop_table.hpp"

#include "hyrise.hpp"

namespace opossum {

DropTable::DropTable(const std::string& table_name, const bool if_exists)
    : AbstractReadOnlyOperator(OperatorType::DropTable), table_name(table_name), if_exists(if_exists) {}

const std::string& DropTable::name() const {
  static const auto name = std::string{"DropTable"};
  return name;
}

std::string DropTable::description(DescriptionMode description_mode) const { return name() + " '" + table_name + "'"; }

std::shared_ptr<const Table> DropTable::_on_execute() {
  // If IF EXISTS is not set and the table is not found, StorageManager throws an exception
  if (!if_exists || Hyrise::get().storage_manager.has_table(table_name)) {
    Hyrise::get().storage_manager.drop_table(table_name);
  }

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> DropTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<DropTable>(table_name, if_exists);
}

void DropTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for DROP TABLE
}

}  // namespace opossum
