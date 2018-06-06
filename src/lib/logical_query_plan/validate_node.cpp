#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::string ValidateNode::description() const { return "[Validate]"; }

std::shared_ptr<AbstractLQPNode> ValidateNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return ValidateNode::make();
}

bool ValidateNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  return true;
}

}  // namespace opossum
