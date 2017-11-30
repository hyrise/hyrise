#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::shared_ptr<AbstractLQPNode> ValidateNode::_clone_impl() const { return std::make_shared<ValidateNode>(); }

std::string ValidateNode::description() const { return "[Validate]"; }

}  // namespace opossum
