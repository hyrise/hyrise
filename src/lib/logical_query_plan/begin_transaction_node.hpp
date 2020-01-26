#pragma once

#include <optional>
#include <string>
#include <vector>

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy node used for the begin transaction statement
 */
class BeginTransactionNode : public EnableMakeForLQPNode<BeginTransactionNode>, public BaseNonQueryNode {
 public:
  BeginTransactionNode();

  std::string description(const DescriptionMode mode) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
