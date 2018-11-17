#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace opossum {

DeleteNode::DeleteNode(const std::string& table_name) : AbstractLQPNode(LQPNodeType::Delete), table_name(table_name) {}

std::string DeleteNode::description() const {
  std::ostringstream desc;

  desc << "[Delete] Table: '" << table_name << "'";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> DeleteNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DeleteNode::make(table_name);
}

bool DeleteNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& delete_node_rhs = static_cast<const DeleteNode&>(rhs);
  return table_name == delete_node_rhs.table_name;
}

}  // namespace opossum
