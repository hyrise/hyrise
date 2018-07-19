#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::string ValidateNode::description() const { return "[Validate]"; }

std::shared_ptr<AbstractLQPNode> ValidateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ValidateNode::make();
}

bool ValidateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
