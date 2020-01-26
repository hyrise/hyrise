#include "commit_transaction_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "expression/value_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

CommitTransactionNode::CommitTransactionNode() : BaseNonQueryNode(LQPNodeType::CommitTransaction) {}

std::string CommitTransactionNode::description(const DescriptionMode mode) const {
  return "LQPNode for CommitTransaction statement";
}

std::shared_ptr<AbstractLQPNode> CommitTransactionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<CommitTransactionNode>();
}

bool CommitTransactionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
