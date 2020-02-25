#include "mutate_meta_table.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "hyrise.hpp"
#include "operators/print.hpp"

namespace opossum {

MutateMetaTable::MutateMetaTable(const std::string& table_name, const MetaTableMutation& mutation_type,
                                 const std::shared_ptr<const AbstractOperator>& values_to_modify,
                                 const std::shared_ptr<const AbstractOperator>& modification_values)
    : AbstractReadOnlyOperator(OperatorType::MutateMetaTable, values_to_modify, modification_values),
      _table_name(table_name.substr(MetaTableManager::META_PREFIX.size())),
      _mutation_type(mutation_type) {}

const std::string& MutateMetaTable::name() const {
  static const auto name = std::string{"Mutate Meta Table"};
  return name;
}

std::shared_ptr<const Table> MutateMetaTable::_on_execute() {
  auto values_to_modify = input_table_left();
  auto modification_values = input_table_right();

  Print::print(values_to_modify);

  std::cout << std::endl;
  Print::print(modification_values);

  switch (_mutation_type) {
    case MetaTableMutation::Insert:
      Hyrise::get().meta_table_manager.insert_into(_table_name, modification_values);
      break;
    case MetaTableMutation::Update:
      Hyrise::get().meta_table_manager.update(_table_name, values_to_modify, modification_values);
      break;
    case MetaTableMutation::Delete:
      Hyrise::get().meta_table_manager.delete_from(_table_name, values_to_modify);
      break;
  }
  return nullptr;
}

std::shared_ptr<AbstractOperator> MutateMetaTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<MutateMetaTable>(_table_name, _mutation_type, copied_input_left, copied_input_right);
}

void MutateMetaTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
