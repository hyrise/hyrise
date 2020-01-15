#include "export_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

ExportNode::ExportNode(const std::string& init_tablename, const std::string& init_filename,
                       const FileType init_file_type)
    : BaseNonQueryNode(LQPNodeType::Export),
      tablename(init_tablename),
      filename(init_filename),
      file_type(init_file_type) {}

std::string ExportNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[Export] Name: '" << tablename << "'";
  return stream.str();
}

size_t ExportNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(tablename);
  boost::hash_combine(hash, filename);
  boost::hash_combine(hash, file_type);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ExportNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ExportNode::make(tablename, filename, file_type);
}

bool ExportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& export_node = static_cast<const ExportNode&>(rhs);
  return tablename == export_node.tablename && filename == export_node.filename && file_type == export_node.file_type;
}

}  // namespace opossum
