#include "create_index_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

CreateIndexNode::CreateIndexNode(const std::string& init_index_name, const bool init_if_not_exists, const std::string& init_table_name, std::shared_ptr<std::vector<ColumnID>> init_column_ids)
    : AbstractNonQueryNode(LQPNodeType::CreateIndex), index_name(init_index_name),
      if_not_exists(init_if_not_exists), table_name(init_table_name), column_ids(init_column_ids) {}

std::string CreateIndexNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
 // TODO: put table name in here
  stream << "[CreateIndex] " << (if_not_exists ? "IfNotExists " : "");
  stream << "Name: '" << index_name << "' ";
  stream << "On Table: '" << table_name << "'";
  return stream.str();
}

size_t CreateIndexNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(index_name);
  boost::hash_combine(hash, if_not_exists);
  boost::hash_combine(hash, column_ids);
  boost::hash_combine(hash, table_name);
  return hash;
}

std::shared_ptr<AbstractLQPNode> CreateIndexNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreateIndexNode::make(index_name, if_not_exists, table_name, column_ids);
}

bool CreateIndexNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_Index_node = static_cast<const CreateIndexNode&>(rhs);
  return index_name == create_Index_node.index_name &&
         if_not_exists == create_Index_node.if_not_exists &&
         table_name == create_Index_node.table_name &&
         *column_ids == *(create_Index_node.column_ids);
}

}  // namespace opossum
