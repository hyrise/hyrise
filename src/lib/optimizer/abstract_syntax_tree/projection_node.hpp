#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class Expression;
struct ColumnID;

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 * It is, however, responsible to calculate arithmetic expressions.
 */
class ProjectionNode : public AbstractASTNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<Expression>>& column_expressions);

  const std::vector<std::shared_ptr<Expression>>& column_expressions() const;

  std::string description(DescriptionMode mode) const override;
  const std::vector<ColumnID>& output_column_ids_to_input_column_ids() const override;
  const std::vector<std::string>& output_column_names() const override;

  std::optional<ColumnID> find_column_id_by_named_column_reference(
      const NamedColumnReference& named_column_reference) const override;

  std::vector<ColumnID> get_output_column_ids_for_table(const std::string& table_name) const override;

  std::string get_qualified_column_name(ColumnID column_id) const override;

  void map_column_ids(const ColumnIDMapping& column_id_mapping, ASTChildSide caller_child_side) override;

 protected:
  void _on_child_changed() override;

 private:
  const std::vector<std::shared_ptr<Expression>> _column_expressions;

  mutable std::optional<std::vector<std::string>> _output_column_names;

  void _update_output() const;
};

}  // namespace opossum
