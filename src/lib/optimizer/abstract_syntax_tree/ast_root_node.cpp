#include "ast_root_node.hpp"

#include <memory>
#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

ASTRootNode::ASTRootNode() : AbstractASTNode(ASTNodeType::Root) {}

std::string ASTRootNode::description() const { return "[ASTRootNode]"; }

std::shared_ptr<AbstractASTNode> ASTRootNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
