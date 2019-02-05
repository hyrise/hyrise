#pragma once

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent deletion (more specifically, invalidation) in a table.
 */
class DeleteNode : public EnableMakeForLQPNode<DeleteNode>, public AbstractLQPNode {
 public:
  DeleteNode();

  std::string description() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
