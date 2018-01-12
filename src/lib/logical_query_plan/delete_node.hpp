#pragma once

#include <memory>
#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * Node type to represent deletion (more specifically, invalidation) in a table.
 */
class DeleteNode : public AbstractLQPNode {
 public:
  explicit DeleteNode(const std::string& table_name);

  std::string description() const override;
  bool subtree_is_read_only() const override;

  const std::string& table_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const override;
  const std::string _table_name;
};

}  // namespace opossum
