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
  explicit StoredTableNode(const std::string& table_name, const optional<std::string>& alias = {});

  const std::string& table_name() const;

  std::string description() const override;
  const std::vector<ColumnID>& output_column_id_to_input_column_id() const override;
  const std::vector<std::string>& output_column_names() const override;

  bool knows_table(const std::string& table_name) const override;

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

  optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

 protected:
  void _on_child_changed() override;

 private:
  const std::shared_ptr<TableStatistics> _gather_statistics() const override;

 private:
  const std::string _table_name;
  const optional<std::string> _alias;

  std::vector<ColumnID> _output_column_id_to_input_column_id;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
