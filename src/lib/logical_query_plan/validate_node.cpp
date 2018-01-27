#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::shared_ptr<AbstractLQPNode> ValidateNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<ValidateNode>();
}

std::string ValidateNode::description() const { return "[Validate]"; }

}  // namespace opossum
