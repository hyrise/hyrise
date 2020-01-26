#include "rollback_transaction_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "expression/value_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

RollbackTransactionNode::RollbackTransactionNode() : BaseNonQueryNode(LQPNodeType::RollbackTransaction) {}

std::string RollbackTransactionNode::description(const DescriptionMode mode) const {
  return "LQPNode for RollbackTransaction statement";
}

std::shared_ptr<AbstractLQPNode> RollbackTransactionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<RollbackTransactionNode>();
}

bool RollbackTransactionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
