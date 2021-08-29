#pragma once

#include "abstract_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "abstract_alter_table_action.hpp"
#include "drop_column_action.hpp"

namespace opossum {


class AlterTableNode : public EnableMakeForLQPNode<AlterTableNode>, public AbstractNonQueryNode {
 public:
  AlterTableNode(const std::string& init_table_name, const std::shared_ptr<AbstractAlterTableAction>& init_alter_action);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const std::shared_ptr<AbstractAlterTableAction> alter_action;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
