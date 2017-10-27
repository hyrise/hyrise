#include "empty_node.hpp"

#include <string>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

EmptyNode::EmptyNode() : AbstractASTNode(ASTNodeType::Empty) {}

std::string EmptyNode::description() const { return "[EmptyNode]"; }

}  // namespace opossum
