#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::string& init_table_name)
    : AbstractNonQueryNode(LQPNodeType::Insert), table_name(init_table_name) {}

std::string InsertNode::description(const DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << table_name << "'";

  return desc.str();
}

size_t InsertNode::_on_shallow_hash() const { return boost::hash_value(table_name); }

std::shared_ptr<AbstractLQPNode> InsertNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return InsertNode::make(table_name);
}

bool InsertNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& insert_node_rhs = static_cast<const InsertNode&>(rhs);
  return table_name == insert_node_rhs.table_name;
}

}  // namespace opossum
