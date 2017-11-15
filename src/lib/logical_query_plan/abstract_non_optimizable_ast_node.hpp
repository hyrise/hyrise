#pragma once

#include "abstract_logical_query_plan_node.hpp"

namespace opossum {

/**
 * This class is a specialization used for nodes that are not intended to be optimized.
 * The current use case is nodes representing management commands, e.g. SHOW TABLES.
 */
class AbstractNonOptimizableASTNode : public AbstractLogicalQueryPlanNode {
 public:
  explicit AbstractNonOptimizableASTNode(LQPNodeType node_type);

  bool is_optimizable() const override;
};

}  // namespace opossum
