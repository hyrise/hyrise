#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type is used to describe SELECT lists for statements that have at least one of the following:
 *  - one or more aggregate functions in their SELECT list
 *  - a GROUP BY clause
 *
 *  The order of the output columns is groupby columns followed by aggregate columns
 */
class AggregateNode : public EnableMakeForLQPNode<AggregateNode>, public AbstractLQPNode {
 public:
  AggregateNode(const std::vector<std::shared_ptr<AbstractExpression>>& group_by_expressions,
                const std::vector<std::shared_ptr<AbstractExpression>>& aggregate_expressions);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;

  /**
   * (1) Forwards left input node's unique constraints if its expressions are a subset of the group-by expressions.
   * (2) Creates a new unique constraint from the group-by expressions if not already existing.
   */
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;

  // Returns non-trivial FDs from the left input node that remain valid.
  std::vector<FunctionalDependency> non_trivial_functional_dependencies() const override;

  // node_expression contains both the group_by- and the aggregate_expressions in that order.
  size_t aggregate_expressions_begin_idx;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
