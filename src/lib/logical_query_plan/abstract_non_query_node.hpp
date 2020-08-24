#pragma once

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Base class for LQP nodes that do not query data (e.g, DML and DDL nodes) and therefore do not output columns.
 *
 * Helper class that provides a output_expressions() override and contains an empty dummy expression vector
 */
class AbstractNonQueryNode : public AbstractLQPNode {
 public:
  using AbstractLQPNode::AbstractLQPNode;

  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;
  std::vector<FunctionalDependency> non_trivial_functional_dependencies() const override;
  bool is_column_nullable(const ColumnID column_id) const override;
};

}  // namespace opossum
