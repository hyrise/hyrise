#include "create_table_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "static_table_node.hpp"

namespace opossum {

CreateTableNode::CreateTableNode(const std::string& table_name, const bool if_not_exists)
    : BaseNonQueryNode(LQPNodeType::CreateTable), table_name(table_name), if_not_exists(if_not_exists) {}

std::string CreateTableNode::description() const {
  std::ostringstream stream;

  stream << "[CreateTable] " << (if_not_exists ? "IfNotExists " : "");
  stream << "Name: '" << table_name << "'";

  return stream.str();
}

size_t CreateTableNode::_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, if_not_exists);
  return hash;
}

std::shared_ptr<AbstractLQPNode> CreateTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreateTableNode::make(table_name, if_not_exists, left_input());
}

bool CreateTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_table_node = static_cast<const CreateTableNode&>(rhs);
  return table_name == create_table_node.table_name && if_not_exists == create_table_node.if_not_exists;
}

}  // namespace opossum
