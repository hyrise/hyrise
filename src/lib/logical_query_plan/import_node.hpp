#pragma once

#include <string>

#include "base_non_query_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "storage/table_column_definition.hpp"

#include "SQLParser.h"

namespace opossum {

/**
 * This node type represents the IMPORT management command.
 */
class ImportNode : public EnableMakeForLQPNode<ImportNode>, public BaseNonQueryNode {
 public:
  ImportNode(const std::string& table_name, const std::string& file_path, const hsql::ImportType file_type);

  std::string description() const override;

  const std::string table_name;
  const std::string file_path;
  const hsql::ImportType file_type;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
