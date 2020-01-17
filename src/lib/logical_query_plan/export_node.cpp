#include "export_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

ExportNode::ExportNode(const std::string& init_table_name, const std::string& init_file_name,
                       const FileType init_file_type)
    : BaseNonQueryNode(LQPNodeType::Export),
      table_name(init_table_name),
      file_name(init_file_name),
      file_type(init_file_type) {}

std::string ExportNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[Export] Name: '" << table_name << "'";
  return stream.str();
}

size_t ExportNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, file_name);
  boost::hash_combine(hash, file_type);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ExportNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ExportNode::make(table_name, file_name, file_type);
}

bool ExportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& export_node = static_cast<const ExportNode&>(rhs);
  return table_name == export_node.table_name && file_name == export_node.file_name &&
         file_type == export_node.file_type;
}

}  // namespace opossum
