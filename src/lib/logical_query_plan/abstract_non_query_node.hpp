#pragma once

#include <memory>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace hyrise {

/**
 * Base class for LQP nodes that do not query data (e.g, DML and DDL nodes) and therefore do not output columns.
 *
 * Helper class that provides overrides for output expressions, data dependencies, and other information we use during
 * query optimization.
 */
class AbstractNonQueryNode : public AbstractLQPNode {
 public:
  using AbstractLQPNode::AbstractLQPNode;

  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const final;

  UniqueColumnCombinations unique_column_combinations() const final;

  OrderDependencies order_dependencies() const final;

  InclusionDependencies inclusion_dependencies() const override;

  FunctionalDependencies non_trivial_functional_dependencies() const final;

  bool is_column_nullable(const ColumnID /*column_id*/) const final;
};

}  // namespace hyrise
