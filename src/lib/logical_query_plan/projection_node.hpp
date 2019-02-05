#pragma once

#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class ProjectionNode : public EnableMakeForLQPNode<ProjectionNode>, public AbstractLQPNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
