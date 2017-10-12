#pragma once

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

// TODO(moritz): Auxiliary code. Keep in src/test?
class MockTableNode : public AbstractASTNode {
 public:
  explicit MockTableNode(const std::shared_ptr<TableStatistics>& statistics);

  const std::vector<ColumnID>& output_column_id_to_input_column_id() const override;
  const std::vector<std::string>& output_column_names() const override;

  bool knows_table(const std::string& table_name) const override;

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

  optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

  std::string description() const override;

 protected:
  void _on_child_changed() override;

 private:
  std::vector<ColumnID> _output_column_id_to_input_column_id;
  std::vector<std::string> _output_column_names;
};
}