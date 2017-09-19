#include "abstract_non_optimizable_ast_node.hpp"

namespace opossum {

AbstractNonOptimizableASTNode::AbstractNonOptimizableASTNode(ASTNodeType node_type) : AbstractASTNode(node_type) {}

bool AbstractNonOptimizableASTNode::is_optimizable() const { return false; }

}  // namespace opossum
