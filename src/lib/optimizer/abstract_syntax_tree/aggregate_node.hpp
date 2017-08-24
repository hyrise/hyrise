#pragma once

#include <memory>
#include <string>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

#include "common.hpp"
#include "types.hpp"

namespace opossum {

class ExpressionNode;

struct AggregateColumnDefinition {
  explicit AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr,
                                     const optional<std::string>& alias = {});

  std::shared_ptr<ExpressionNode> expr;
  optional<std::string> alias;
};

/**
 * This node type is used to describe SELECT lists for statements that have at least one of the following:
 *  - one or more aggregate functions in their SELECT list
 *  - a GROUP BY clause
 *
 *  The order of the output columns is groupby columns followed by aggregate columns
 */
class AggregateNode : public AbstractASTNode {
 public:
  explicit AggregateNode(const std::vector<AggregateColumnDefinition>& aggregates,
                         const std::vector<ColumnID>& groupby_columns);

  const std::vector<AggregateColumnDefinition>& aggregates() const;
  const std::vector<ColumnID>& groupby_columns() const;

  std::string description() const override;
  const std::vector<std::string>& output_column_names() const override;
  const std::vector<ColumnID>& output_column_ids() const override;

  optional<ColumnID> find_column_id_for_column_identifier(const ColumnIdentifier& column_identifier) const override;

  optional<ColumnID> find_column_id_for_expression(const std::shared_ptr<ExpressionNode> & expression) const override;

 protected:
  void _on_child_changed() override;

 private:
  std::vector<AggregateColumnDefinition> _aggregates;
  std::vector<ColumnID> _groupby_columns;

  std::vector<ColumnID> _output_column_ids;
  std::vector<std::string> _output_column_names;
};

}  // namespace opossum
