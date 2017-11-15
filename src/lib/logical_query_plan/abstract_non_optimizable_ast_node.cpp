#include "abstract_non_optimizable_ast_node.hpp"

namespace opossum {

AbstractNonOptimizableASTNode::AbstractNonOptimizableASTNode(LQPNodeType node_type) : AbstractLogicalQueryPlanNode(node_type) {}

bool AbstractNonOptimizableASTNode::is_optimizable() const { return false; }

}  // namespace opossum
