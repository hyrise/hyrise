#include "insert_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

InsertNode::InsertNode(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env, const std::string& init_table_name)
    : AbstractNonQueryNode(LQPNodeType::Insert), hyrise_env(init_hyrise_env), table_name(init_table_name) {}

std::string InsertNode::description(const DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Insert] Into table '" << table_name << "'";

  return desc.str();
}

size_t InsertNode::_on_shallow_hash() const { return boost::hash_value(table_name); }

std::shared_ptr<AbstractLQPNode> InsertNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return InsertNode::make(hyrise_env, table_name);
}

bool InsertNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& insert_node_rhs = static_cast<const InsertNode&>(rhs);
  return hyrise_env == insert_node_rhs.hyrise_env && table_name == insert_node_rhs.table_name;
}

}  // namespace opossum
