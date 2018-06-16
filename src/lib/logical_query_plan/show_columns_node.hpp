#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the SHOW COLUMNS management command.
 */
class ShowColumnsNode : public EnableMakeForLQPNode<ShowColumnsNode>, public AbstractLQPNode {
 public:
  explicit ShowColumnsNode(const std::string& table_name);

  std::string description() const override;

  const std::string& table_name() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping & node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const override;

 private:
  const std::string _table_name;
};

}  // namespace opossum
