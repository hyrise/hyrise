#include "drop_table.hpp"

#include "hyrise.hpp"

namespace hyrise {

DropTable::DropTable(const std::string& init_table_name, const bool init_if_exists)
    : AbstractReadOnlyOperator(OperatorType::DropTable), table_name(init_table_name), if_exists(init_if_exists) {}

const std::string& DropTable::name() const {
  static const auto name = std::string{"DropTable"};
  return name;
}

std::string DropTable::description(DescriptionMode description_mode) const {
  return AbstractOperator::description(description_mode) + " '" + table_name + "'";
}

std::shared_ptr<const Table> DropTable::_on_execute() {
  // If IF EXISTS is not set and the table is not found, StorageManager throws an exception
  if (!if_exists || Hyrise::get().storage_manager.has_table(table_name)) {
    Hyrise::get().storage_manager.drop_table(table_name);
  }

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> DropTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<DropTable>(table_name, if_exists);
}

void DropTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for DROP TABLE
}

}  // namespace hyrise
