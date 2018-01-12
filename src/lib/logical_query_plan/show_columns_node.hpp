#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW COLUMNS management command.
 */
class ShowColumnsNode : public AbstractLQPNode {
 public:
  explicit ShowColumnsNode(const std::string& table_name);

  std::string description() const override;

  const std::string& table_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_child,
      const std::shared_ptr<AbstractLQPNode>& copied_right_child) const override;

 private:
  const std::string _table_name;
};

}  // namespace opossum
