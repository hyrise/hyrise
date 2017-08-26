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
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractASTNode(ASTNodeType::Aggregate), _aggregates(aggregates), _groupby_column_ids(groupby_column_ids) {}

const std::vector<AggregateColumnDefinition>& AggregateNode::aggregates() const { return _aggregates; }

const std::vector<ColumnID>& AggregateNode::groupby_column_ids() const { return _groupby_column_ids; }

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

  if (!_groupby_column_ids.empty()) {
    s << " GROUP BY [";
    for (const auto& column_name : _groupby_column_ids) {
      s << column_name << ", ";
    }
    s << "]";
  }

  return s.str();
}

void AggregateNode::_on_child_changed() {
  DebugAssert(!!left_child(), "AggregateNode needs a child.");

  _output_column_names.clear();
  _output_column_ids.clear();

  _output_column_names.reserve(_groupby_column_ids.size() + _aggregates.size());
  _output_column_ids.reserve(_groupby_column_ids.size() + _aggregates.size());

  /**
   * Set output column ids and names.
   *
   * The Aggregate operator will put all GROUP BY columns in the output table at the beginning,
   * so we first handle those, and afterwards add the column information for the aggregate functions.
   */
  ColumnID column_id{0};
  for (const auto groupby_column_id : _groupby_column_ids) {
    _output_column_ids.emplace_back(column_id);
    column_id++;

    _output_column_names.emplace_back(left_child()->output_column_names()[groupby_column_id]);
  }

  for (const auto& aggregate : _aggregates) {
    DebugAssert(aggregate.expr->type() == ExpressionType::FunctionIdentifier, "Expression must be a function.");

    std::string column_name;

    if (aggregate.alias) {
      column_name = *aggregate.alias;
    } else {
      /**
       * If the aggregate function has no alias defined in the query, we simply parse the expression back to a string.
       * SQL in the standard does not specify a name to be given.
       * This might result in multiple output columns with the same name, but we accept that.
       * Other DBs behave similarly (e.g. MySQL).
       */
      column_name = aggregate.expr->to_string(left_child());
    }

    _output_column_names.emplace_back(column_name);
    _output_column_ids.emplace_back(INVALID_COLUMN_ID);
  }
}

const std::vector<std::string>& AggregateNode::output_column_names() const { return _output_column_names; }

const std::vector<ColumnID>& AggregateNode::output_column_ids() const { return _output_column_ids; }

optional<ColumnID> AggregateNode::find_column_id_for_column_identifier(
    const ColumnIdentifier& column_identifier) const {
  DebugAssert(!!left_child(), "AggregateNode needs a child.");

  // TODO(mp) Handle column_identifier having a table that is this node's alias

  /*
   * Search for ColumnIdentifier in Aggregate columns ALIASes, if the column_identifier has no table:
   * These columns are created by the Aggregate Operator, so we have to look through them here.
   */
  optional<ColumnID> column_id_aggregate;
  if (!column_identifier.table_name) {
    for (uint16_t i = 0; i < _aggregates.size(); i++) {
      const auto& aggregate_definition = _aggregates[i];

      // If AggregateDefinition has no alias, column_name will not match.
      if (column_identifier.column_name == aggregate_definition.alias) {
        // Check that we haven't found a match yet.
        Assert(!column_id_aggregate, "Column name " + column_identifier.column_name + " is ambiguous.");
        // Aggregate columns come after groupby columns in the Aggregate's output
        column_id_aggregate = ColumnID{static_cast<ColumnID::base_type>(i + _groupby_column_ids.size())};
      }
    }
  }

  /*
   * Search for ColumnIdentifier in Group By columns:
   * These columns have been created by another node. Since Aggregates can only have a single child node,
   * we just have to check the left_child for the ColumnIdentifier.
   */
  auto column_id_groupby = left_child()->find_column_id_for_column_identifier(column_identifier);
  if (column_id_groupby) {
    auto iter = std::find(_groupby_column_ids.begin(), _groupby_column_ids.end(), *column_id_groupby);
    Assert(iter != _groupby_column_ids.end(), "Requested column identifier is not in GroupBy list");
  }

  Assert(!column_id_aggregate || !column_id_groupby, "Column name " + column_identifier.column_name + " is ambiguous.");

  if (column_id_aggregate) {
    return column_id_aggregate;
  }

  // Optional might not be set.
  return column_id_groupby;
}

optional<ColumnID> AggregateNode::find_column_id_for_expression(
    const std::shared_ptr<ExpressionNode>& expression) const {
  const auto iter = std::find_if(_aggregates.begin(), _aggregates.end(), [&](const auto& rhs) {
    DebugAssert(!!rhs.expr, "No expr in AggregateColumnDefinition");
    return *expression == *rhs.expr;
  });

  if (iter == _aggregates.end()) {
    return nullopt;
  }

  const auto idx = std::distance(_aggregates.begin(), iter);
  return ColumnID{static_cast<ColumnID::base_type>(idx + _groupby_column_ids.size())};
}

}  // namespace opossum
