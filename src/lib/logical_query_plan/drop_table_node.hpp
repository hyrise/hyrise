#pragma once

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

class DropTableNode : public EnableMakeForLQPNode<DropTableNode>, public BaseNonQueryNode {
 public:
  DropTableNode(const std::string& table_name, bool if_exists);

  std::string description() const override;

  const std::string table_name;
  const bool if_exists;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
