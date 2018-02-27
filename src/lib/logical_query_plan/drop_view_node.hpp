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
  bool subplan_is_read_only() const override;
  const std::vector<std::string>& output_column_names() const override;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

  const std::string& view_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;

 private:
  const std::string _view_name;
};

}  // namespace opossum
