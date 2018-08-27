#include "show_cxlumns_node.hpp"

#include <string>

namespace opossum {

ShowCxlumnsNode::ShowCxlumnsNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::ShowCxlumns), _table_name(table_name) {}

std::string ShowCxlumnsNode::description() const { return "[ShowCxlumns] Table: '" + _table_name + "'"; }

const std::string& ShowCxlumnsNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractLQPNode> ShowCxlumnsNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ShowCxlumnsNode::make(_table_name);
}

bool ShowCxlumnsNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& show_cxlumns_node_rhs = static_cast<const ShowCxlumnsNode&>(rhs);
  return _table_name == show_cxlumns_node_rhs._table_name;
}

}  // namespace opossum
