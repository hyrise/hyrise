#include "change_meta_table.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "hyrise.hpp"

namespace opossum {

ChangeMetaTable::ChangeMetaTable(const std::string& table_name, const MetaTableChangeType& change_type,
                                 const std::shared_ptr<const AbstractOperator>& values_to_modify,
                                 const std::shared_ptr<const AbstractOperator>& modification_values)
    : AbstractReadWriteOperator(OperatorType::ChangeMetaTable, values_to_modify, modification_values),
      _meta_table_name(table_name.substr(MetaTableManager::META_PREFIX.size())),
      _change_type(change_type) {}

const std::string& ChangeMetaTable::name() const {
  static const auto name = std::string{"Change Meta Table"};
  return name;
}

std::shared_ptr<const Table> ChangeMetaTable::_on_execute(std::shared_ptr<TransactionContext> context) {
  Assert(context->is_auto_commit(), "Meta tables cannot be modified during transactions");

  switch (_change_type) {
    case MetaTableChangeType::Insert:
      Hyrise::get().meta_table_manager.insert_into(_meta_table_name, input_table_right());
      break;
    case MetaTableChangeType::Update:
      Hyrise::get().meta_table_manager.update(_meta_table_name, input_table_left(), input_table_right());
      break;
    case MetaTableChangeType::Delete:
      Hyrise::get().meta_table_manager.delete_from(_meta_table_name, input_table_left());
      break;
  }

  return nullptr;
}

std::shared_ptr<AbstractOperator> ChangeMetaTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ChangeMetaTable>(_meta_table_name, _change_type, copied_input_left, copied_input_right);
}

void ChangeMetaTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
