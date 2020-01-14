#include "export_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"
// #include "static_table_node.hpp"

namespace opossum {

ExportNode::ExportNode(const std::string& init_tablename, const std::string& init_filename,
                       const FileType init_filetype)
    : BaseNonQueryNode(LQPNodeType::Export),
      tablename(init_tablename),
      filename(init_filename),
      filetype(init_filetype) {}

std::string ExportNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[Exort] Name: '" << tablename << "'";
  return stream.str();
}

size_t ExportNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(tablename);
  boost::hash_combine(hash, filename);
  boost::hash_combine(hash, filetype);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ExportNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ExportNode::make(tablename, filename, filetype);
}

bool ExportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& export_node = static_cast<const ExportNode&>(rhs);
  return tablename == export_node.tablename && filename == export_node.filename && filetype == export_node.filetype;
}

}  // namespace opossum
