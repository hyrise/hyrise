#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

/**
 * This node type is used to represent any type of Join, including cross products.
 */
class JoinNode : public EnableMakeForLQPNode<JoinNode>, public AbstractLQPNode {
 public:
  // Constructor for Cross Joins. join_mode has to be JoinMode::Cross
  explicit JoinNode(const JoinMode init_join_mode);

  // Utility constructor that just calls the multi predicated constructor
  JoinNode(const JoinMode init_join_mode, const std::shared_ptr<AbstractExpression>& join_predicate);

  // Constructor for multi predicated joins
  JoinNode(const JoinMode init_join_mode, const std::vector<std::shared_ptr<AbstractExpression>>& init_join_predicates);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  /**
   * (1) Forwards left input node's unique constraints for JoinMode::Semi and JoinMode::AntiNullAsTrue/False
   * (2) Discards all input unique constraints for Cross Joins, Multi-Predicate Joins and Non-Equi-Joins
   * (3) Forwards selected input unique constraints for Inner and Outer Equi-Joins based on join column uniqueness.
   */
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;

  /**
   * (a) Semi- & Anti-Joins:
   *      - Forwards left input node's non-trivial FDs
   * (b) Cross-Joins:
   *      - Forwards non-trivial FDs from both input nodes.
   * (c) Inner-/Outer-Joins:
   *      - Forwards non-trivial FDs from both input nodes whose determinant expressions stay non-nullable.
   *      - Turns derived, trivial FDs from the left and/or right input node into non-trivial FDs if the underlying
   *        unique constraints do not survive the join.
   */
  std::vector<FunctionalDependency> non_trivial_functional_dependencies() const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates() const;

  JoinMode join_mode;

 protected:
  /**
   * @return A subset of the given LQPUniqueConstraints @param left_unique_constraints and @param
   *         right_unique_constraints that remains valid despite the join operation.
   *         Depending on the join columns,
   *          (a) the left,
   *          (b) the right,
   *          (c) both or
   *          (d) none
   *         of the given unique constraint sets are returned.
   * Please note: This helper function can be called for all joins, except for Semi- and Anti-Join types.
   */
  std::shared_ptr<LQPUniqueConstraints> _output_unique_constraints(
      const std::shared_ptr<LQPUniqueConstraints>& left_unique_constraints,
      const std::shared_ptr<LQPUniqueConstraints>& right_unique_constraints) const;

  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
