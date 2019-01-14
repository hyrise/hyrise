#pragma once

#include "abstract_lqp_node.hpp" // NEEDEDINCLUDE
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SortNode : public EnableMakeForLQPNode<SortNode>, public AbstractLQPNode {
 public:
  explicit SortNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                    const std::vector<OrderByMode>& order_by_modes);

  std::string description() const override;

  const std::vector<OrderByMode> order_by_modes;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
