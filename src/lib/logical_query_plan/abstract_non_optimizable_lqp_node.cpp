#include "abstract_non_optimizable_lqp_node.hpp"

namespace opossum {

AbstractNonOptimizableLQPNode::AbstractNonOptimizableLQPNode(LQPNodeType node_type) : AbstractLQPNode(node_type) {}

bool AbstractNonOptimizableLQPNode::is_optimizable() const { return false; }

}  // namespace opossum
