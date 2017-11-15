#include "ast_root_node.hpp"

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

ASTRootNode::ASTRootNode() : AbstractLogicalPlanNode(LQPNodeType::Root) {}

std::string ASTRootNode::description() const { return "[ASTRootNode]"; }

}  // namespace opossum
