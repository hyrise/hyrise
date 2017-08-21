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
    : AbstractASTNode(ASTNodeType::Aggregate), _aggregates(aggregates), _groupby_columns(groupby_columns) {}

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

void AggregateNode::_on_child_changed() {
  // TODO(tim): BLOCKING - debug assert left child
  // also, when set chiild is called, information must be cleared!
  /**
   * Set output column ids and names.
   *
   * The Aggregate operator will put all GROUP BY columns in the output table at the beginning,
   * so we first handle those, and afterwards add the column information for the aggregate functions.
   */
  for (const auto groupby_column_id : _groupby_columns) {
    _output_column_ids.emplace_back(groupby_column_id);
    _output_column_names.emplace_back(left_child()->output_column_names()[groupby_column_id]);
  }

  /**
   * Find the maximum ColumnID of the GROUP BY columns.
   * This is required to generate new ColumnIDs for all aggregate functions,
   * for which new columns will be created by the Aggregate operator.
   */
  auto iter = std::max_element(_groupby_columns.cbegin(), _groupby_columns.cend());
  auto current_column_id = static_cast<uint16_t>(*iter);

  for (const auto& aggregate : _aggregates) {
    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionReference, "Expression must be a function.");

    std::string alias;

    if (aggregate.alias) {
      alias = *aggregate.alias;
    } else {
      /**
       * If the aggregate function has no alias defined in the query, we simply parse the expression back to a string.
       * SQL in the standard does not specify a name to be given.
       * This might result in multiple output columns with the same name, but we accept that.
       * Other DBs behave similarly (e.g. MySQL).
       */
      alias = aggregate.expr->to_string(left_child());
    }

    _output_column_names.emplace_back(alias);

    // All AggregateFunctions create a new column, which has to get its own ColumnID.
    current_column_id++;
    _output_column_ids.emplace_back(current_column_id);
  }
}

const std::vector<std::string>& AggregateNode::output_column_names() const { return _output_column_names; }

const std::vector<ColumnID>& AggregateNode::output_column_ids() const { return _output_column_ids; }

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

}  // namespace opossum
