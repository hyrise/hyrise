#include "validate_node.hpp"

#include <string>

namespace opossum {

std::shared_ptr<ValidateNode> ValidateNode::make(const std::shared_ptr<AbstractLQPNode>& child) {
  const auto validate_node = std::make_shared<ValidateNode>();
  validate_node->set_left_child(child);
  return validate_node;
}

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::shared_ptr<AbstractLQPNode> ValidateNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<ValidateNode>();
}

std::string ValidateNode::description() const { return "[Validate]"; }

bool ValidateNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  return true;
}

}  // namespace opossum
