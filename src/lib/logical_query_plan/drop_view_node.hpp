#pragma once

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent deleting a view from the StorageManager
 */
class DropViewNode : public EnableMakeForLQPNode<DropViewNode>, public AbstractLQPNode {
 public:
  explicit DropViewNode(const std::string& view_name);

  std::string description() const override;

  const std::string& view_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  const std::string _view_name;
};

}  // namespace opossum
