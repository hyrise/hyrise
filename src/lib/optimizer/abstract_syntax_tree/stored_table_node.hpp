#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

struct ColumnID;
class TableStatistics;

/**
 * This node type represents a table stored by the table manager.
 * They are the leafs of every meaningful AST tree.
 */
class StoredTableNode : public AbstractASTNode {
 public:
  explicit StoredTableNode(const std::string& table_name);

  std::string description() const override;

  std::vector<ColumnID> output_column_ids() const override;

  const std::string& table_name() const;

  bool find_column_id_for_column_name(std::string & column_name, ColumnID &column_id) override;

 private:
  const std::shared_ptr<TableStatistics> _gather_statistics() const override;
  const std::string _table_name;
};

}  // namespace opossum
