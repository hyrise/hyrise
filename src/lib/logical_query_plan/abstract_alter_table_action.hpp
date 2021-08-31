#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

class AbstractAlterTableAction {
 public:
  explicit AbstractAlterTableAction(hsql::AlterAction init_action);
  virtual ~AbstractAlterTableAction();
  virtual size_t on_shallow_hash() = 0;
  virtual bool on_shallow_equals(AbstractAlterTableAction& rhs) = 0;
  virtual std::string description() = 0;
  hsql::ActionType type;
  hsql::AlterAction action;
};

}  // namespace opossum
