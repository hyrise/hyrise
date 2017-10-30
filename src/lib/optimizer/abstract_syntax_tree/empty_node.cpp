#include "empty_node.hpp"

#include <memory>
#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

EmptyNode::EmptyNode() : AbstractASTNode(ASTNodeType::Empty) {}

std::string EmptyNode::description() const { return "[EmptyNode]"; }

std::shared_ptr<AbstractASTNode> EmptyNode::clone_subtree() const {
  return _clone_without_subclass_members<decltype(*this)>();
}

}  // namespace opossum
