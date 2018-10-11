#include "drop_table_node.hpp"

namespace opossum {

DropTableNode::DropTableNode(const std::string& table_name)
    : BaseNonQueryNode(LQPNodeType::DropTable), table_name(table_name) {}

std::string DropTableNode::description() const { return std::string("[DropTable] Name: '") + table_name + "'"; }

std::shared_ptr<AbstractLQPNode> DropTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DropTableNode::make(table_name);
}

bool DropTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& drop_table_node = static_cast<const DropTableNode&>(rhs);
  return table_name == drop_table_node.table_name;
}

}  // namespace opossum
