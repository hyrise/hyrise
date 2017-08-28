#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class TableStatistics;

/**
 * This node type represents a table stored by the table manager.
 * They are the leafs of every meaningful AST tree.
 */
class StoredTableNode : public AbstractASTNode {
 public:
  explicit StoredTableNode(const std::string& table_name);

  std::string description() const override;

  std::vector<std::string> output_column_names() const override;

  const std::string& table_name() const;

 private:
  const std::shared_ptr<TableStatistics> _gather_statistics() const override;
  const std::string _table_name;
};

}  // namespace opossum
