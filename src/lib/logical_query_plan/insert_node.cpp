#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string table_name) : AbstractLQPNode(LQPNodeType::Insert), _table_name(table_name) {}

std::shared_ptr<AbstractLQPNode> InsertNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return InsertNode::make(_table_name);
}

std::string InsertNode::description() const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << _table_name << "'";

  return desc.str();
}

bool InsertNode::subplan_is_read_only() const { return false; }

const std::string& InsertNode::table_name() const { return _table_name; }

bool InsertNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& insert_node = static_cast<const InsertNode&>(rhs);

  return _table_name == insert_node._table_name;
}

}  // namespace opossum
