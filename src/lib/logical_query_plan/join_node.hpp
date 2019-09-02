#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

/**
 * This node type is used to represent any type of Join, including cross products.
 */
class JoinNode : public EnableMakeForLQPNode<JoinNode>, public AbstractLQPNode {
 public:
  // Constructor for Cross Joins. join_mode has to be JoinMode::Cross
  explicit JoinNode(const JoinMode join_mode);

  // Utility constructor that just calls the multi predicated constructor
  JoinNode(const JoinMode join_mode, const std::shared_ptr<AbstractExpression>& join_predicate);

  // Constructor for multi predicated joins
  JoinNode(const JoinMode join_mode, const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates);

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates() const;

  const JoinMode join_mode;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  mutable std::vector<std::shared_ptr<AbstractExpression>> _column_expressions;
};

}  // namespace opossum
