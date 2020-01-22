#include "begin_transaction_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "expression/value_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

BeginTransactionNode::BeginTransactionNode() : BaseNonQueryNode(LQPNodeType::BeginTransaction) {}

std::string BeginTransactionNode::description(const DescriptionMode mode) const {
  return "LQPNode for BeginTransaction statement";
}

std::shared_ptr<AbstractLQPNode> BeginTransactionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<BeginTransactionNode>();
}

bool BeginTransactionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
