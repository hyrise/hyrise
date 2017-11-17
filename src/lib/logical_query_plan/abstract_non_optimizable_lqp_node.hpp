#pragma once

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This class is a specialization used for nodes that are not intended to be optimized.
 * The current use case is nodes representing management commands, e.g. SHOW TABLES.
 */
class AbstractNonOptimizableLQPNode : public AbstractLQPNode {
 public:
  explicit AbstractNonOptimizableLQPNode(LQPNodeType node_type);

  bool is_optimizable() const override;
};

}  // namespace opossum
