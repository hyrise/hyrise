#include "create_index_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

CreateIndexNode::CreateIndexNode(const std::string& init_index_name, const std::string& init_indexed_table_name, const bool init_if_not_exists, std::vector<std::string> init_column_names)
    : AbstractNonQueryNode(LQPNodeType::CreateIndex), index_name(init_index_name), indexed_table_name(init_indexed_table_name), if_not_exists(init_if_not_exists), column_names(init_column_names) {}

std::string CreateIndexNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;

  stream << "[CreateIndex] " << (if_not_exists ? "IfNotExists " : "");
  stream << "Name: '" << index_name << "' ";
  stream << "Table: '" << indexed_table_name << "'";
  return stream.str();
}

size_t CreateIndexNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(index_name);
  boost::hash_combine(hash, indexed_table_name);
  boost::hash_combine(hash, if_not_exists);
  boost::hash_combine(hash, column_names);
  return hash;
}

std::shared_ptr<AbstractLQPNode> CreateIndexNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreateIndexNode::make(index_name, indexed_table_name, if_not_exists, column_names);
}

bool CreateIndexNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_Index_node = static_cast<const CreateIndexNode&>(rhs);
  return index_name == create_Index_node.index_name &&
         indexed_table_name == create_Index_node.indexed_table_name &&
         if_not_exists == create_Index_node.if_not_exists &&
         column_names == create_Index_node.column_names;
}

}  // namespace opossum
