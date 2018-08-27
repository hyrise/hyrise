#pragma once

#include <string>

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/lqp_view.hpp"

namespace opossum {

/**
 * This node type represents the CREATE VIEW management command.
 */
class CreateViewNode : public EnableMakeForLQPNode<CreateViewNode>, public BaseNonQueryNode {
 public:
  CreateViewNode(const std::string& view_name, const std::shared_ptr<LQPView>& view);

  std::string description() const override;

  std::string view_name() const;
  std::shared_ptr<LQPView> view() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;

 private:
  const std::string _view_name;
  const std::shared_ptr<LQPView> _view;
};

}  // namespace opossum
