#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/expression_node.hpp"
#include "types.hpp"

namespace opossum {

struct AggregateColumnDefinition {
  AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr);
  AggregateColumnDefinition(const std::string& alias, const std::shared_ptr<ExpressionNode>& expr);

  optional<std::string> alias;
  std::shared_ptr<ExpressionNode> expr;
};

class AggregateNode : public AbstractAstNode {
 public:
  explicit AggregateNode(const std::vector<AggregateColumnDefinition> aggregates,
                         const std::vector<std::string>& groupby_columns);

  const std::vector<AggregateColumnDefinition>& aggregates() const { return _aggregates; }
  const std::vector<std::string>& groupby_columns() const { return _groupby_columns; }

  std::string description() const override;

  const std::vector<std::string>& output_columns() const override;

 private:
  std::vector<AggregateColumnDefinition> _aggregates;
  std::vector<std::string> _groupby_columns;
};

}  // namespace opossum
