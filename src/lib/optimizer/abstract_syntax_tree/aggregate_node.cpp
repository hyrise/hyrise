#include "aggregate_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression/expression_node.hpp"

#include "common.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateColumnDefinition::AggregateColumnDefinition(const std::shared_ptr<ExpressionNode>& expr,
                                                     const optional<std::string>& alias)
    : expr(expr), alias(alias) {}

AggregateNode::AggregateNode(const std::vector<AggregateColumnDefinition>& aggregates,
                             const std::vector<ColumnID>& groupby_columns)
    : AbstractASTNode(ASTNodeType::Aggregate), _aggregates(aggregates), _groupby_columns(groupby_columns) {

  _output_column_ids = groupby_columns;

//  for (const auto& aggregate : aggregates) {
//    std::string alias;
//    if (aggregate.alias) {
//      alias = *aggregate.alias;
//    } else {
//      // If the aggregate function has no alias defined in the query, we simply name it like the function.
//      // This might result in multiple output columns with the same name, but Postgres is doing things the same way.
//      DebugAssert(aggregate.expr->type() == ExpressionType::FunctionReference, "Expression must be a function.");
//      alias = aggregate.expr->name();
//    }
//
//    _output_column_ids.emplace_back(alias);
//  }

}

const std::vector<AggregateColumnDefinition>& AggregateNode::aggregates() const { return _aggregates; }

const std::vector<ColumnID>& AggregateNode::groupby_columns() const { return _groupby_columns; }

std::string AggregateNode::description() const {
  std::ostringstream s;

  auto stream_aggregate = [&](const AggregateColumnDefinition& aggregate) {
    s << aggregate.expr->to_expression_string();
    if (aggregate.alias) s << "AS \"" << (*aggregate.alias) << "\"";
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

std::vector<ColumnID> AggregateNode::output_column_ids() const { return _output_column_ids; }

bool AggregateNode::find_column_id_for_column_name(std::string & column_name, ColumnID &column_id) {
  std::vector<ColumnID> matches;
  for (size_t i = 0; i < _aggregates.size(); i++) {
    const auto &aggregate_definition = _aggregates[i];
    if (column_name == aggregate_definition.alias) {
      matches.emplace_back(ColumnID{i});
    }
  }

  if (left_child()->find_column_id_for_column_name(column_name, column_id)) {
    if (std::find(_groupby_columns.begin(), _groupby_columns.end(), column_id) != _groupby_columns.end()) {
      matches.emplace_back(column_id);
    }
  }

  if (matches.size() != 1) {
    Fail("Either did not find column name or column name is ambiguous.");
  }

  column_id = matches[0];
  return true;
}

}  // namespace opossum
