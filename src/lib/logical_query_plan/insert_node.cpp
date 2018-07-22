#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string& table_name) : AbstractLQPNode(LQPNodeType::Insert), _table_name(table_name) {}

std::string InsertNode::description() const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << _table_name << "'";

  return desc.str();
}

const std::string& InsertNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractLQPNode> InsertNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return InsertNode::make(_table_name);
}

bool InsertNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& insert_node_rhs = static_cast<const InsertNode&>(rhs);
  return _table_name == insert_node_rhs._table_name;
}

}  // namespace opossum
