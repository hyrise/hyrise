#include "ast_root_node.hpp"

#include <string>

namespace opossum {

ASTRootNode::ASTRootNode() : AbstractASTNode(ASTNodeType::Root) {}

std::string ASTRootNode::description() const { return "ASTRootNode"; }

}  // namespace opossum
