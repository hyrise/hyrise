#include "import_node.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "static_table_node.hpp"

namespace opossum {

ImportNode::ImportNode(const std::string& init_tablename, const std::string& init_filename,
                       const FileType init_filetype)
    : BaseNonQueryNode(LQPNodeType::Import),
      tablename(init_tablename),
      filename(init_filename),
      filetype(init_filetype) {}

std::string ImportNode::description(const DescriptionMode mode) const {
  std::ostringstream stream;
  stream << "[Import] Name: '" << tablename << "'";
  return stream.str();
}

size_t ImportNode::_on_shallow_hash() const {
  auto hash = boost::hash_value(tablename);
  boost::hash_combine(hash, filename);
  boost::hash_combine(hash, filetype);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ImportNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ImportNode::make(tablename, filename, filetype);
}

bool ImportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& import_node = static_cast<const ImportNode&>(rhs);
  return tablename == import_node.tablename && filename == import_node.filename && filetype == import_node.filetype;
}

}  // namespace opossum
