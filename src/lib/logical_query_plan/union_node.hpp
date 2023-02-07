#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * This node type is used to:
 * (1) Represent the UNION set operation in two modes, Unique and All.
 * (2) Intersect disjunctive PosLists (using the Positions mode).
 *     For example, `a = 1 OR b = 2` can be split up into two PredicateNodes, which unite in a UnionNode with
 *     SetOperationMode::Positions.
 */

class UnionNode : public EnableMakeForLQPNode<UnionNode>, public AbstractLQPNode {
 public:
  explicit UnionNode(const SetOperationMode init_set_operation_mode);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  /**
   * (1) Forwards unique column combinations from the left input node in case of SetOperationMode::Positions.
   *     (UCCs of both, left and right input node are identical)
   * (2) Discards all input unique column combinations for SetOperationMode::All and
   * (3) Fails for SetOperationMode::Unique, which is not yet implemented.
   */
  UniqueColumnCombinations unique_column_combinations() const override;

  OrderDependencies order_dependencies() const override;

  InclusionDependencies inclusion_dependencies() const override;

  // Implementation is limited to SetOperationMode::Positions only. Passes FDs from the left input node.
  FunctionalDependencies non_trivial_functional_dependencies() const override;

  const SetOperationMode set_operation_mode;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};
}  // namespace hyrise
