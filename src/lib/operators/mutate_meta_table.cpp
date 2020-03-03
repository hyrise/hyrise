#include "mutate_meta_table.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "hyrise.hpp"

namespace opossum {

MutateMetaTable::MutateMetaTable(const std::string& table_name, const MetaTableMutation& mutation_type,
                                 const std::shared_ptr<const AbstractOperator>& values_to_modify,
                                 const std::shared_ptr<const AbstractOperator>& modification_values)
    : AbstractReadWriteOperator(OperatorType::MutateMetaTable, values_to_modify, modification_values),
      _table_name(table_name.substr(MetaTableManager::META_PREFIX.size())),
      _mutation_type(mutation_type) {}

const std::string& MutateMetaTable::name() const {
  static const auto name = std::string{"Mutate Meta Table"};
  return name;
}

std::shared_ptr<const Table> MutateMetaTable::_on_execute(std::shared_ptr<TransactionContext> context) {
  // Meta tables can only be modified during non-transactions
  if (context->auto_commit() == AutoCommit::No) {
    _mark_as_failed();
    return nullptr;
  }

  switch (_mutation_type) {
    case MetaTableMutation::Insert:
      Hyrise::get().meta_table_manager.insert_into(_table_name, input_table_right());
      break;
    case MetaTableMutation::Update:
      Hyrise::get().meta_table_manager.update(_table_name, input_table_left(), input_table_right());
      break;
    case MetaTableMutation::Delete:
      Hyrise::get().meta_table_manager.delete_from(_table_name, input_table_left());
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
