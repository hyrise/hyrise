#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

class AbstractAlterTableAction {
 public:
  virtual ~AbstractAlterTableAction();
  virtual size_t on_shallow_hash();
  virtual std::shared_ptr<AbstractAlterTableAction> on_shallow_copy();
  virtual bool on_shallow_equals(const AbstractAlterTableAction& rhs);
};

}  // namespace opossum
