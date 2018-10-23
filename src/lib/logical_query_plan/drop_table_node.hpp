#pragma once

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

class DropTableNode : public EnableMakeForLQPNode<DropTableNode>, public BaseNonQueryNode {
 public:
  explicit DropTableNode(const std::string& table_name);

  std::string description() const override;

  const std::string table_name;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
