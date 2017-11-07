#include "validate_node.hpp"

#include <string>

namespace opossum {

ValidateNode::ValidateNode() : AbstractASTNode(ASTNodeType::Validate) {}

std::string ValidateNode::description(DescriptionMode mode) const { return "[Validate]"; }

}  // namespace opossum
