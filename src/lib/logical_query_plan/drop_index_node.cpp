#include "drop_index_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

DropIndexNode::DropIndexNode(const std::string& init_index_name, const std::string& init_table_name)
    : AbstractNonQueryNode(LQPNodeType::DropIndex), index_name(init_index_name), table_name(init_table_name) {}

std::string DropIndexNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[DropIndex] ";
  stream << "Name: '" << index_name << "'";
  stream << " On table: '" << table_name << "'";
  return stream.str();
}

size_t DropIndexNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(index_name);
  boost::hash_combine(hash, table_name);
  return hash;
}

std::shared_ptr<AbstractLQPNode> DropIndexNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DropIndexNode::make(index_name, table_name);
}

bool DropIndexNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& drop_index_node = static_cast<const DropIndexNode&>(rhs);
  return index_name == drop_index_node.index_name &&
      table_name == drop_index_node.table_name;
}

}  // namespace opossum
