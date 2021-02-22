#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

class HyriseEnvironmentRef;

class DropTableNode : public EnableMakeForLQPNode<DropTableNode>, public AbstractNonQueryNode {
 public:
  DropTableNode(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env, const std::string& init_table_name,
                bool init_if_exists);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::shared_ptr<HyriseEnvironmentRef> hyrise_env;
  const std::string table_name;
  const bool if_exists;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
