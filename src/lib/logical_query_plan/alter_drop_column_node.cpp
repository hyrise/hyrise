#include "alter_drop_column_node.hpp"

namespace opossum {

AlterDropColumnNode::AlterDropColumnNode(const std::string& init_table_name, const std::string& init_column_name, const bool init_if_exists)
    : AbstractNonQueryNode(LQPNodeType::AlterTableDropColumn), table_name(init_table_name), column_name(init_column_name),
      if_column_exists(init_if_exists) {}

std::string AlterDropColumnNode::description(const DescriptionMode mode) const {
  return std::string("[DropColumn] Table: '") + table_name + "'" + "; Column: " + column_name + "'";
}

size_t AlterDropColumnNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, if_column_exists);
  boost::hash_combine(hash, column_name);
  return hash;
}

std::shared_ptr<AbstractLQPNode> AlterDropColumnNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return AlterDropColumnNode::make(table_name, column_name, if_column_exists);
}

bool AlterDropColumnNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& alter_drop_column_node = static_cast<const AlterDropColumnNode&>(rhs);
  return table_name == alter_drop_column_node.table_name && column_name == alter_drop_column_node.column_name && alter_drop_column_node.if_column_exists == if_column_exists;
}

}  // namespace opossum
