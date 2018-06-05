#pragma once

#include <string>

#include "abstract_lqp_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents the CREATE VIEW management command.
 */
class CreateViewNode : public AbstractLQPNode {
 public:
  explicit CreateViewNode(const std::string& view_name, const std::shared_ptr<AbstractLQPNode>& lqp);

  std::string description() const override;

  std::string view_name() const;
  std::shared_ptr<AbstractLQPNode> lqp() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping & node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const override;

 private:
  const std::string _view_name;
  const std::shared_ptr<AbstractLQPNode> _lqp;
};

}  // namespace opossum
