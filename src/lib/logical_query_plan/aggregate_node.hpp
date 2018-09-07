#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"
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

  std::string description() const override;
  const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions() const override;
  std::vector<std::shared_ptr<AbstractExpression>> node_expressions() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input) const override;

  const std::vector<std::shared_ptr<AbstractExpression>> group_by_expressions;
  const std::vector<std::shared_ptr<AbstractExpression>> aggregate_expressions;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  std::vector<std::shared_ptr<AbstractExpression>> _column_expressions;
};

}  // namespace opossum
