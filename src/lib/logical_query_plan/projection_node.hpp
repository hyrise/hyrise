#pragma once

#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class ProjectionNode : public EnableMakeForLQPNode<ProjectionNode>, public AbstractLQPNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  /**
   * Forwards unique constraints from the left input node that fulfill the following criteria:
   *  - unique constraint's expressions remain part of the ProjectionNode's output expressions
   */
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;

  // Returns non-trivial FDs from the left input node that remain valid.
  std::vector<FunctionalDependency> non_trivial_functional_dependencies() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
