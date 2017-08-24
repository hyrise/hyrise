#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/expression/expression_node.hpp"

namespace opossum {

struct ColumnID;

/**
 * Node type to represent common projections, i.e. without any aggregate functionality.
 * It is, however, responsible to calculate arithmetic expressions.
 */
class ProjectionNode : public AbstractASTNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<ExpressionNode>>& column_expressions);

  const std::vector<std::shared_ptr<ExpressionNode>>& column_expressions() const;

  std::string description() const override;
  const std::vector<ColumnID>& output_column_ids() const override;
  const std::vector<std::string>& output_column_names() const override;

  optional<ColumnID> find_column_id_for_column_identifier(const ColumnIdentifier& column_identifier) const override;

  optional<ColumnID> find_column_id_for_expression(const std::shared_ptr<ExpressionNode> & expression) const override;

 protected:
  void _on_child_changed() override;

 private:
  const std::vector<std::shared_ptr<ExpressionNode>> _column_expressions;
  std::vector<ColumnID> _output_column_ids;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
