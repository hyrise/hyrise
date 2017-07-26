#include "aggregate_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

AggregateColumnDefinition::AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr) : expr(expr) {}

AggregateColumnDefinition::AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr,
                                                     const std::string& alias)
    : expr(expr), alias(alias) {}

AggregateNode::AggregateNode(const std::vector<AggregateColumnDefinition> aggregates,
                             const std::vector<std::string>& groupby_columns)
    : AbstractASTNode(ASTNodeType::Aggregate), _aggregates(aggregates), _groupby_columns(groupby_columns) {
  for (const auto aggregate : aggregates) {
    std::string alias;
    if (aggregate.alias)
      alias = *aggregate.alias;
    else
      alias = "TODO";  // aggregate.expr->to_alias_name();  // TODO(mp)

    _output_column_names.emplace_back(alias);
  }

  for (const auto& groupby_column : groupby_columns) {
    _output_column_names.emplace_back(groupby_column);
  }
}

std::string AggregateNode::description() const {
  std::ostringstream s;

  s << "Aggregate: ";
  for (const auto & aggregate : _aggregates) {
    s << aggregate.expr->to_expression_string();
    if (aggregate.alias) s << "AS '" << (*aggregate.alias) << "'";
    // HAAACKY! but works
    if (aggregate.expr != _aggregates.back().expr) s << ", ";
  }

  if (!_groupby_columns.empty()) {
    s << " GROUP BY ";
    for (const auto & column_name : _groupby_columns) {
      s << column_name << ", ";
    }
  }

  return s.str();
}

std::vector<std::string> AggregateNode::output_column_names() const { return _output_column_names; }

}  // namespace opossum
