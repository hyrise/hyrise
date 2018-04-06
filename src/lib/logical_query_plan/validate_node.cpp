#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

AbstractLQPNodeSPtr ValidateNode::_deep_copy_impl(
    const AbstractLQPNodeSPtr& copied_left_input,
    const AbstractLQPNodeSPtr& copied_right_input) const {
  return ValidateNode::make();
}

std::string ValidateNode::description() const { return "[Validate]"; }

bool ValidateNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  return true;
}

}  // namespace opossum
