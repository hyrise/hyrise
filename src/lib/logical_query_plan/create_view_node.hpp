#pragma once

#include <string>

#include "abstract_lqp_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/view.hpp"

namespace opossum {

/**
 * This node type represents the CREATE VIEW management command.
 */
class CreateViewNode : public AbstractLQPNode {
 public:
  CreateViewNode(const std::string& view_name, const std::shared_ptr<View>& view);

  std::string description() const override;

  std::string view_name() const;
  std::shared_ptr<View> view() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping & node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const override;

 private:
  const std::string _view_name;
  const std::shared_ptr<View> _view;
};

}  // namespace opossum
