#include "aggregate_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression/expression_node.hpp"

#include "common.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateColumnDefinition::AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr,
                                                     const optional<std::string>& alias)
    : expr(expr), alias(alias) {}

AggregateNode::AggregateNode(const std::vector<AggregateColumnDefinition>& aggregates,
                             const std::vector<std::string>& groupby_columns)
    : AbstractASTNode(ASTNodeType::Aggregate), _aggregates(aggregates), _groupby_columns(groupby_columns) {
  for (const auto& aggregate : aggregates) {
    std::string alias;
    if (aggregate.alias) {
      alias = *aggregate.alias;
    } else {
      // If the aggregate function has no alias defined in the query, we simply name it like the function.
      // This might result in multiple output columns with the same name, but Postgres is doing things the same way.
      DebugAssert(aggregate.expr->type() == ExpressionType::FunctionReference, "Expression must be a function.");
      alias = aggregate.expr->name();
    }

    _output_column_names.emplace_back(alias);
  }

  _output_column_names = groupby_columns;
}

const std::vector<AggregateColumnDefinition>& AggregateNode::aggregates() const { return _aggregates; }

const std::vector<std::string>& AggregateNode::groupby_columns() const { return _groupby_columns; }

std::string AggregateNode::description() const {
  std::ostringstream s;

  auto stream_aggregate = [&](const AggregateColumnDefinition& aggregate) {
    s << aggregate.expr->to_expression_string();
    if (aggregate.alias) s << "AS '" << (*aggregate.alias) << "'";
  };

  auto it = _aggregates.begin();
  if (it != _aggregates.end()) stream_aggregate(*it);
  for (; it != _aggregates.end(); ++it) {
    s << ", ";
    stream_aggregate(*it);
  }

  if (!_groupby_columns.empty()) {
    s << " GROUP BY [";
    for (const auto& column_name : _groupby_columns) {
      s << column_name << ", ";
    }
    s << "]";
  }

  return s.str();
}

std::vector<std::string> AggregateNode::output_column_names() const { return _output_column_names; }

}  // namespace opossum
