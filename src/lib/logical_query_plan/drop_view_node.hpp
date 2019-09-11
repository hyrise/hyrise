#pragma once

#include <memory>
#include <string>

#include "base_non_query_node.hpp"

namespace opossum {

/**
 * Node type to represent deleting a view from the StorageManager
 */
class DropViewNode : public EnableMakeForLQPNode<DropViewNode>, public BaseNonQueryNode {
 public:
  DropViewNode(const std::string& view_name, bool if_exists);

  std::string description() const override;

  const std::string view_name;
  const bool if_exists;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
