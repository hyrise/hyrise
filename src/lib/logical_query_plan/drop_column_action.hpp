#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "abstract_alter_table_action.hpp"

namespace opossum {

class DropColumnAction : public AbstractAlterTableAction {
 public:
  DropColumnAction(const hsql::DropColumnAction init_alter_action);

  std::string column_name;
  bool if_exists;
  hsql::DropColumnAction drop_column_action;

  size_t on_shallow_hash() override;
  std::shared_ptr<AbstractAlterTableAction> on_shallow_copy() override;
  bool on_shallow_equals(AbstractAlterTableAction& rhs) override;
  std::string description() override;
};

}  // namespace opossum
