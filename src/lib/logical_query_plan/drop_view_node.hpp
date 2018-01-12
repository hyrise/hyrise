#pragma once

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent deleting a view from the StorageManager
 */
class DropViewNode : public AbstractLQPNode {
 public:
  explicit DropViewNode(const std::string& view_name);

  std::string description() const override;
  bool subtree_is_read_only() const override;

  const std::string& view_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const override;
  const std::string _view_name;
};

}  // namespace opossum
