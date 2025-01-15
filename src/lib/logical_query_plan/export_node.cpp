#include "export_node.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/container_hash/hash.hpp>

#include "magic_enum.hpp"

#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"
#include "storage/encoding_type.hpp"

namespace hyrise {

ExportNode::ExportNode(const std::string& init_file_name, const FileType init_file_type, const std::optional<EncodingType> init_file_encoding)
    : AbstractNonQueryNode(LQPNodeType::Export), file_name(init_file_name), file_type(init_file_type), file_encoding(init_file_encoding) {}

std::string ExportNode::description(const DescriptionMode /*mode*/) const {
  auto file_type_str = std::string{magic_enum::enum_name(file_type)};
  boost::algorithm::to_lower(file_type_str);
  
  if (file_encoding) {
    auto file_encoding_str = std::string{magic_enum::enum_name(*file_encoding)};
    boost::algorithm::to_lower(file_encoding_str);

    return "[Export] to '" + file_name + "' (" + file_type_str + ") with encoding (" + file_encoding_str + ")";
  }
  return "[Export] to '" + file_name + "' (" + file_type_str + ")";
}

size_t ExportNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, file_name);
  boost::hash_combine(hash, file_type);
  boost::hash_combine(hash, file_encoding);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ExportNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return ExportNode::make(file_name, file_type, file_encoding);
}

bool ExportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& export_node = static_cast<const ExportNode&>(rhs);
  return file_name == export_node.file_name && file_type == export_node.file_type && file_encoding == export_node.file_encoding;
}

}  // namespace hyrise
