#include "ast_root_node.hpp"

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

ASTRootNode::ASTRootNode() : AbstractASTNode(ASTNodeType::Root) {}

std::string ASTRootNode::description(DescriptionMode mode) const { return "[ASTRootNode]"; }

}  // namespace opossum
