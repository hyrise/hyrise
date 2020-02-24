#pragma once

#include <string>

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "import_export/file_type.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This node type represents the COPY TO management command.
 */
class ExportNode : public EnableMakeForLQPNode<ExportNode>, public BaseNonQueryNode {
 public:
  ExportNode(const std::string& init_table_name, const std::string& init_file_name, const FileType init_file_type);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const std::string file_name;
  const FileType file_type;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
