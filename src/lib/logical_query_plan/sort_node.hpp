#pragma once

#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"
#include "types.hpp"

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
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
