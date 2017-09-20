#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public AbstractASTNode {
 public:
  DummyTableNode();

  std::string description() const override;

  const std::vector<ColumnID>& output_column_id_to_input_column_id() const override;
  const std::vector<std::string>& output_column_names() const override;

  optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

 protected:
  void _on_child_changed() override;

  std::vector<ColumnID> _output_column_id_to_input_column_id;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
