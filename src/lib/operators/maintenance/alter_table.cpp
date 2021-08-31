#include "alter_table.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "drop_column_impl.hpp"

namespace opossum {

AlterTable::AlterTable(const std::string& init_table_name, const std::shared_ptr<AbstractAlterTableAction>& init_alter_action)
    : AbstractReadWriteOperator(OperatorType::AlterTable),
      target_table_name(init_table_name),
      action(init_alter_action) {
  _impl = _create_impl();
}

const std::string& AlterTable::name() const {
  static const auto name = std::string{"AlterTable"};
  return name;
}

std::string AlterTable::description(DescriptionMode description_mode) const {
  std::ostringstream stream;
  //TODO:
  stream << AbstractOperator::description(description_mode) << " '" + target_table_name << "' " << _impl->description();
  return stream.str();
}

std::shared_ptr<const Table> AlterTable::_on_execute(std::shared_ptr<TransactionContext> context) {
  _impl->on_execute(context);
  return std::make_shared<Table>(TableColumnDefinitions{{"OK", DataType::Int, false}}, TableType::Data);  // Dummy table
}

std::shared_ptr<AbstractOperator> AlterTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<AlterTable>(target_table_name, action);
}

void AlterTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No parameters possible for ALTER TABLE
}

std::shared_ptr<AbstractAlterTableImpl> AlterTable::_create_impl() {
  switch(action->type) {
    case hsql::ActionType::DROPCOLUMN:
      return std::make_shared<DropColumnImpl>(target_table_name, action);
  }
}

}  // namespace opossum
