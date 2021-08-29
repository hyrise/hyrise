#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "drop_column_action.hpp"

namespace opossum {

DropColumnAction::DropColumnAction(const hsql::DropColumnAction init_alter_action) : alter_action(init_alter_action) {
  column_name = alter_action.columnName;
  if_exists = alter_action.ifExists;
}

size_t DropColumnAction::on_shallow_hash(){
  return 1;
}

std::shared_ptr<AbstractAlterTableAction> DropColumnAction::on_shallow_copy(){
  return std::make_shared<DropColumnAction>(alter_action);
}

bool on_shallow_equals(const AbstractAlterTableAction& rhs) {
  return false
}

}  // namespace opossum
