#pragma once

#include "abstract_lqp_node.hpp"

namespace hyrise {

/**
 * LQPNode to wrap SQL:2003 window functions. The information necessary to define the window and the window function is
 * stored in WindowExpression and WindowFunctionExpression, so this LQPNode is rather lightweight.
 */
class WindowNode : public EnableMakeForLQPNode<WindowNode>, public AbstractLQPNode {
 public:
  explicit WindowNode(const std::shared_ptr<AbstractExpression>& window_function_expression);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  // Forwards left input node's unique column combinations.
  UniqueColumnCombinations unique_column_combinations() const override;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
