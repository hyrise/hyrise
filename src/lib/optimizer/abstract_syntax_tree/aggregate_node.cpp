#include "aggregate_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

AggregateColumnDefinition::AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr) : expr(expr) {}

AggregateColumnDefinition::AggregateColumnDefinition(const std::string& alias,
                                                     const std::shared_ptr<ExpressionNode>& expr)
    : alias(alias), expr(expr) {}

AggregateNode::AggregateNode(const std::vector<AggregateColumnDefinition> aggregates,
                             const std::vector<std::string>& groupby_columns)
    : AbstractAstNode(AstNodeType::Aggregate), _aggregates(aggregates), _groupby_columns(groupby_columns) {
  for (const auto aggregate : aggregates) {
    std::string alias;
    if (aggregate.alias)
      alias = *aggregate.alias;
    else
      alias = "TODO";  // aggregate.expr->to_alias_name();  // TODO(mp)

    _output_columns.emplace_back(alias);
  }

  for (const auto& groupby_column : groupby_columns) {
    _output_columns.emplace_back(groupby_column);
  }
}

std::string AggregateNode::description() const { return "Aggregate"; }

const std::vector<std::string>& AggregateNode::output_columns() const { return _output_columns; }
}  // namespace opossum
