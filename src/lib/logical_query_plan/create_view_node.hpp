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
  CreateViewNode(const std::string& view_name, const std::shared_ptr<LQPView>& view, bool if_not_exists);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string view_name;
  const std::shared_ptr<LQPView> view;
  const bool if_not_exists;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
