#include "validate_node.hpp"

#include <memory>
#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractASTNode(ASTNodeType::Validate) {}

std::string ValidateNode::description() const { return "[Validate]"; }

std::shared_ptr<AbstractASTNode> ValidateNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
