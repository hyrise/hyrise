#pragma once

#include <optional>
#include <string>
#include <vector>

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy node used for the rollback transaction statement
 */
class RollbackTransactionNode : public EnableMakeForLQPNode<RollbackTransactionNode>, public BaseNonQueryNode {
 public:
  RollbackTransactionNode();

  std::string description(const DescriptionMode mode) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
