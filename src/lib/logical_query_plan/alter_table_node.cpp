#include "alter_table_node.hpp"
#include "drop_column_action.hpp"

namespace opossum {

AlterTableNode::AlterTableNode(const std::string& init_table_name, const std::shared_ptr<AbstractAlterTableAction>& init_alter_action)
    : AbstractNonQueryNode(LQPNodeType::AlterTable),
      table_name(init_table_name),
      alter_action(init_alter_action) {}

std::string AlterTableNode::description(const DescriptionMode mode) const {
  return std::string("[AlterTable] Table: '") + table_name + "'" + "; " + alter_action->description();
}

size_t AlterTableNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, alter_action->on_shallow_hash());
  return hash;
}

std::shared_ptr<AbstractLQPNode> AlterTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return AlterTableNode::make(table_name, alter_action);
}

bool AlterTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& alter_table_node = static_cast<const AlterTableNode&>(rhs);
  return table_name == alter_table_node.table_name && alter_action->on_shallow_equals(*alter_table_node.alter_action);
}

}  // namespace opossum
