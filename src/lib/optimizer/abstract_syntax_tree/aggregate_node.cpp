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
  /**
   * Set output column ids and names.
   */
  for (const auto groupby_column_id : groupby_columns) {
    _output_column_ids.emplace_back(groupby_column_id);
    _output_column_names.emplace_back(left_child()->output_column_names()[groupby_column_id]);
  }

  for (const auto& aggregate : aggregates) {
    std::string alias;

    if (aggregate.alias) {
      alias = *aggregate.alias;
    } else {
      // If the aggregate function has no alias defined in the query, we simply name it like the function.
      // SQL in the standard does not specify a name to be given.
      // This might result in multiple output columns with the same name (same as Postgres).
      DebugAssert(aggregate.expr->type() == ExpressionType::FunctionReference, "Expression must be a function.");
      alias = aggregate.expr->name();
    }

    _output_column_names.emplace_back(alias);
    // TODO(tim): BLOCKING - complete, and verify.
    _output_column_ids.emplace_back();
  }
}

const std::vector<AggregateColumnDefinition>& AggregateNode::aggregates() const { return _aggregates; }

const std::vector<ColumnID>& AggregateNode::groupby_columns() const { return _groupby_columns; }

std::string AggregateNode::description() const {
  std::ostringstream s;

  auto stream_aggregate = [&](const AggregateColumnDefinition& aggregate) {
    s << aggregate.expr->to_string();
    if (aggregate.alias) s << " AS \"" << (*aggregate.alias) << "\"";
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

optional<ColumnID> AggregateNode::find_column_id_for_column_identifier(
    const ColumnIdentifier& column_identifier) const {
  DebugAssert(!!left_child(), "AggregateNode needs a child.");

  /*
   * Search for ColumnIdentifier in Aggregate columns:
   * These columns are created by the Aggregate Operator, so we have to look through them here.
   */
  optional<ColumnID> column_id_aggregate;
  for (uint16_t i = 0; i < _aggregates.size(); i++) {
    const auto& aggregate_definition = _aggregates[i];

    // If AggregateDefinition has no alias, column_name will not match.
    if (column_identifier.column_name == aggregate_definition.alias) {
      // Check that we haven't found a match yet.
      Assert(!column_id_aggregate, "Column name " + column_identifier.column_name + " is ambiguous.");
      column_id_aggregate = ColumnID{i};
    }
  }

  /*
   * Search for ColumnIdentifier in Group By columns:
   * These columns have been created by another node. Since Aggregates can only have a single child node,
   * we just have to check the left_child for the ColumnIdentifier.
   */
  auto column_id_groupby = left_child()->find_column_id_for_column_identifier(column_identifier);
  Assert(!column_id_aggregate || !column_id_groupby, "Column name " + column_identifier.column_name + " is ambiguous.");

  if (column_id_aggregate) {
    return column_id_aggregate;
  }

  // Optional might not be set.
  return column_id_groupby;
}

std::vector<std::string> AggregateNode::output_column_names() const { return _output_column_names; }

}  // namespace opossum
