#pragma once

#include <memory>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace hyrise {

class GatherStatisticsNode : public EnableMakeForLQPNode<GatherStatisticsNode>, public AbstractLQPNode {
 public:
  explicit GatherStatisticsNode(const std::shared_ptr<AbstractExpression>& init_expression);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  UniqueColumnCombinations unique_column_combinations() const override;

  OrderDependencies order_dependencies() const override;

  std::shared_ptr<AbstractExpression> expression() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
