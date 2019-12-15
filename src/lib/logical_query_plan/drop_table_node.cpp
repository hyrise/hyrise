#include "drop_table_node.hpp"

namespace opossum {

DropTableNode::DropTableNode(const std::string& table_name, const bool if_exists)
    : BaseNonQueryNode(LQPNodeType::DropTable), table_name(table_name), if_exists(if_exists) {}

std::string DropTableNode::description(const DescriptionMode mode) const {
  return std::string("[DropTable] Name: '") + table_name + "'";
}

size_t DropTableNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, if_exists);
  return hash;
}

std::shared_ptr<AbstractLQPNode> DropTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DropTableNode::make(table_name, if_exists);
}

bool DropTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& drop_table_node = static_cast<const DropTableNode&>(rhs);
  return table_name == drop_table_node.table_name && if_exists == drop_table_node.if_exists;
}

}  // namespace opossum
