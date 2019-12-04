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
  ImportNode(const std::string& init_tablename,
  	          const std::string& init_filename,
              const hsql::ImportType init_filetype);

  std::string description() const override;

  const std::string tablename;
  const std::string filename;
  const hsql::ImportType filetype;

 protected:
  size_t _shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
