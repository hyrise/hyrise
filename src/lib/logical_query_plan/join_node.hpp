#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace hyrise {

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

  /**
   * @returns true if this JoinNode was added added by the SemiJoinReductionRule for increased performance.
   */
  bool is_semi_reduction() const;

  /**
   * @pre     The SemiJoinReductionRule must have added this JoinNode to the LQP, so that ::is_semi_reduction returns
   *          true.
   * @returns a shared pointer to the JoinNode for which the semi join reduction was added as a pre-filter.
   *          If the internal weak pointer to the reduced join is not set or expired, the LQP will be traversed upwards
   *          until the reduced join has been found, otherwise the function fails.
   */
  std::shared_ptr<JoinNode> get_or_find_reduced_join_node() const;

  /**
   * Sets the `is_semi_reduction` property of this JoinNode to true, and stores a weak pointer to the
   * @param reduced_join which gets pre-filtered by this semi join reduction.
   * Note: This function is meant to be called by the SemiJoinReductionRule, which adds semi join reductions to LQPs.
   */
  void mark_as_semi_reduction(const std::shared_ptr<JoinNode>& reduced_join);

  JoinMode join_mode;

 protected:
  /**
   * The following data members are only relevant for semi joins added by the SemiJoinReductionRule. For details,
   * read the documentation of ::mark_as_semi_reduction and ::get_or_find_reduced_join_node.
   */
  bool _is_semi_reduction = false;
  mutable std::weak_ptr<JoinNode> _reduced_join_node;

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

}  // namespace hyrise
