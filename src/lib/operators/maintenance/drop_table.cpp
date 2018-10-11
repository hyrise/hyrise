#include "drop_table.hpp"

#include "storage/storage_manager.hpp"

namespace opossum {

DropTable::DropTable(const std::string& table_name)
    : AbstractReadOnlyOperator(OperatorType::DropTable), table_name(table_name) {}

const std::string DropTable::name() const { return "Drop Table"; }

const std::string DropTable::description(DescriptionMode description_mode) const {
  return name() + " '" + table_name + "'";
}

std::shared_ptr<const Table> DropTable::_on_execute() {
  StorageManager::get().drop_table(table_name);

  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> DropTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<DropTable>(table_name);
}

void DropTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for DROP TABLE
}

}  // namespace opossum
