#include "import_node.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/container_hash/hash.hpp>

#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/abstract_non_query_node.hpp"

namespace hyrise {

ImportNode::ImportNode(const std::string& init_table_name, const std::string& init_file_name,
                       const FileType init_file_type, const std::optional<EncodingType> init_table_encoding)
    : AbstractNonQueryNode(LQPNodeType::Import),
      table_name(init_table_name),
      file_name(init_file_name),
      file_type(init_file_type),
      table_encoding(init_table_encoding) {}

std::string ImportNode::description(const DescriptionMode /*mode*/) const {
  auto file_type_str = std::string{magic_enum::enum_name(file_type)};
  boost::algorithm::to_lower(file_type_str);

  if (table_encoding) {
    auto file_encoding_str = std::string{magic_enum::enum_name(*table_encoding)};
    boost::algorithm::to_lower(file_encoding_str);
    return "[Import] Name: '" + table_name + "' from '" + file_name + "' (" + file_type_str + ") using encoding (" +
           file_encoding_str + ")";
  }

  return "[Import] Name: '" + table_name + "' from '" + file_name + "' (" + file_type_str + ")";
}

size_t ImportNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, table_name);
  boost::hash_combine(hash, file_name);
  boost::hash_combine(hash, file_type);
  boost::hash_combine(hash, table_encoding);
  return hash;
}

std::shared_ptr<AbstractLQPNode> ImportNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return ImportNode::make(table_name, file_name, file_type, table_encoding);
}

bool ImportNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& import_node = static_cast<const ImportNode&>(rhs);
  return table_name == import_node.table_name && file_name == import_node.file_name &&
         file_type == import_node.file_type && table_encoding == import_node.table_encoding;
}

}  // namespace hyrise
