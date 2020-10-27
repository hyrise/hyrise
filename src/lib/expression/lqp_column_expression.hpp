#pragma once

#include "abstract_expression.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

class LQPColumnExpression : public AbstractExpression {
 public:
  explicit LQPColumnExpression(const std::shared_ptr<const AbstractLQPNode>& original_node,
                               const ColumnID original_column_id);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;
  bool requires_computation() const override;

  // Needs to be weak since nodes can store LQPColumnExpressions referring to themselves (e.g., for
  // StoredTableNode::output_expressions). If the original_node is not referenced by any shared_ptr anymore, it is
  // deleted. As a result, the weak_ptr expires. It should not be accessed anymore. Thus, if original_node.lock() is
  // a nullptr, the LQP is defective.
  const std::weak_ptr<const AbstractLQPNode> original_node;
  const ColumnID original_column_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
};

}  // namespace opossum
