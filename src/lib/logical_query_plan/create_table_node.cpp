#include "create_table_node.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"

namespace hyrise {

CreateTableNode::CreateTableNode(const std::string& init_table_name, const bool init_if_not_exists)
    : AbstractNonQueryNode(LQPNodeType::CreateTable), table_name(init_table_name), if_not_exists(init_if_not_exists) {}

std::string CreateTableNode::description(const DescriptionMode /*mode*/) const {
  auto stream = std::ostringstream{};
  stream << "[CreateTable] " << (if_not_exists ? "IfNotExists " : "");
  stream << "Name: '" << table_name << "'";
  return stream.str();
}

size_t CreateTableNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, table_name);
  boost::hash_combine(hash, if_not_exists);
  return hash;
}

std::shared_ptr<AbstractLQPNode> CreateTableNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return CreateTableNode::make(table_name, if_not_exists, left_input());
}

bool CreateTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& create_table_node = static_cast<const CreateTableNode&>(rhs);
  return table_name == create_table_node.table_name && if_not_exists == create_table_node.if_not_exists;
}

}  // namespace hyrise
