#include "import_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

ImportNode::ImportNode(const std::string& init_table_name, const std::string& init_file_name,
                       const FileType init_file_type)
    : BaseNonQueryNode(LQPNodeType::Import),
      table_name(init_table_name),
      file_name(init_file_name),
      file_type(init_file_type) {}

std::string ImportNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[Import] Name: '" << table_name << "'";
  return stream.str();
}

size_t ImportNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, file_name);
  boost::hash_combine(hash, file_type);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ImportNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ImportNode::make(table_name, file_name, file_type);
}

bool ImportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& import_node = static_cast<const ImportNode&>(rhs);
  return table_name == import_node.table_name && file_name == import_node.file_name &&
         file_type == import_node.file_type;
}

}  // namespace opossum
