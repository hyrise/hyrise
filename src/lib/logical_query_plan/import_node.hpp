#pragma once

#include <memory>
#include <string>

#include "abstract_non_query_node.hpp"
#include "import_export/file_type.hpp"

namespace hyrise {

/**
 * This node type represents the IMPORT / COPY FROM management command.
 */
class ImportNode : public EnableMakeForLQPNode<ImportNode>, public AbstractNonQueryNode {
 public:
  ImportNode(const std::string& init_table_name, const std::string& init_file_name, const FileType init_file_type);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string table_name;
  const std::string file_name;
  const FileType file_type;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const override;
};

}  // namespace hyrise
