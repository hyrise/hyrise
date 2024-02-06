#include "insert_node.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

InsertNode::InsertNode(const std::string& init_table_name)
    : AbstractNonQueryNode(LQPNodeType::Insert), table_name(init_table_name) {}

std::string InsertNode::description(const DescriptionMode /*mode*/) const {
  auto desc = std::ostringstream{};
  desc << "[Insert] Into table '" << table_name << "'";
  return desc.str();
}

size_t InsertNode::_on_shallow_hash() const {
  return std::hash<std::string>{}(table_name);
}

std::shared_ptr<AbstractLQPNode> InsertNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return InsertNode::make(table_name);
}

bool InsertNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& insert_node_rhs = static_cast<const InsertNode&>(rhs);
  return table_name == insert_node_rhs.table_name;
}

}  // namespace hyrise
