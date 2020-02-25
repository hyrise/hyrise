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
    : AbstractReadWriteOperator(OperatorType::MutateMetaTable, values_to_modify, modification_values),
      _table_name(table_name.substr(MetaTableManager::META_PREFIX.size())),
      _mutation_type(mutation_type) {}

const std::string& MutateMetaTable::name() const {
  static const auto name = std::string{"Mutate Meta Table"};
  return name;
}

std::shared_ptr<const Table> MutateMetaTable::_on_execute(std::shared_ptr<TransactionContext> context) {
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

void MutateMetaTable::_on_commit_records(const CommitID cid) {
  // TO DO: should we do the actual operations here? Could be crucial if accessing changed values in same transaction,
  // but doing this before could be crucial for other transactions
}
void MutateMetaTable::_on_rollback_records() {
  switch (_mutation_type) {
    case MetaTableMutation::Delete:
      Hyrise::get().meta_table_manager.insert_into(_table_name, input_table_left());
      break;
    case MetaTableMutation::Update:
      Hyrise::get().meta_table_manager.update(_table_name, input_table_right(), input_table_left());
      break;
    case MetaTableMutation::Insert:
      Hyrise::get().meta_table_manager.delete_from(_table_name, input_table_right());
      break;
  }
}

std::shared_ptr<AbstractOperator> MutateMetaTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<MutateMetaTable>(_table_name, _mutation_type, copied_input_left, copied_input_right);
}

void MutateMetaTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
