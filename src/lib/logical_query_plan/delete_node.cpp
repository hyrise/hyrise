#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

DeleteNode::DeleteNode(const std::string& table_name) : AbstractLQPNode(LQPNodeType::Delete), _table_name(table_name) {}

AbstractLQPNodeSPtr DeleteNode::_deep_copy_impl(
    const AbstractLQPNodeSPtr& copied_left_input,
    const AbstractLQPNodeSPtr& copied_right_input) const {
  return DeleteNode::make(_table_name);
}

std::string DeleteNode::description() const {
  std::ostringstream desc;

  desc << "[Delete] Table: '" << _table_name << "'";

  return desc.str();
}

bool DeleteNode::subplan_is_read_only() const { return false; }

const std::string& DeleteNode::table_name() const { return _table_name; }

bool DeleteNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& delete_node = static_cast<const DeleteNode&>(rhs);

  return _table_name == delete_node._table_name;
}

}  // namespace opossum
