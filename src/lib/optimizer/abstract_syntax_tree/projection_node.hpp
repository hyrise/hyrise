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
  std::vector<ColumnID> output_column_ids() const override;
  std::vector<std::string> output_column_names() const override;
  optional<ColumnID> find_column_id_for_column_identifier(const ColumnIdentifier& column_identifier) const override;

 private:
  void _set_output_information() const;

 private:
  const std::vector<std::shared_ptr<ExpressionNode>> _column_expressions;
  mutable std::vector<ColumnID> _output_column_ids;
  mutable std::vector<std::string> _output_column_names;
};

}  // namespace opossum
