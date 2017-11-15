#include "aggregate_node.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "optimizer/expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateNode::AggregateNode(const std::vector<std::shared_ptr<Expression>>& aggregate_expressions,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractLogicalQueryPlanNode(LQPNodeType::Aggregate),
      _aggregate_expressions(aggregate_expressions),
      _groupby_column_ids(groupby_column_ids) {
  for ([[gnu::unused]] const auto& expression : aggregate_expressions) {
    DebugAssert(expression->type() == ExpressionType::Function, "Aggregate expression must be a function.");
  }
}

const std::vector<std::shared_ptr<Expression>>& AggregateNode::aggregate_expressions() const {
  return _aggregate_expressions;
}

const std::vector<ColumnID>& AggregateNode::groupby_column_ids() const { return _groupby_column_ids; }

std::string AggregateNode::description() const {
  std::ostringstream s;

  s << "[Aggregate] ";

  std::vector<std::string> verbose_column_names;
  if (left_child()) {
    verbose_column_names = left_child()->get_verbose_column_names();
  }

  auto stream_aggregate = [&](const std::shared_ptr<Expression>& aggregate_expr) {
    s << aggregate_expr->to_string(verbose_column_names);

    if (aggregate_expr->alias()) {
      s << " AS \"" << (*aggregate_expr->alias()) << "\"";
    }
  };

  auto aggregates_it = _aggregate_expressions.begin();
  if (aggregates_it != _aggregate_expressions.end()) {
    stream_aggregate(*aggregates_it);
    ++aggregates_it;
  }

  for (; aggregates_it != _aggregate_expressions.end(); ++aggregates_it) {
    s << ", ";
    stream_aggregate(*aggregates_it);
  }

  if (!_groupby_column_ids.empty()) {
    s << " GROUP BY [";

    for (size_t group_by_idx = 0; group_by_idx < _groupby_column_ids.size(); ++group_by_idx) {
      if (left_child()) {
        s << left_child()->get_verbose_column_name(_groupby_column_ids[group_by_idx]);
        if (group_by_idx + 1 < _groupby_column_ids.size()) {
          s << ", ";
        }
      }
    }
    s << "]";
  }

  return s.str();
}

std::string AggregateNode::get_verbose_column_name(ColumnID column_id) const {
  DebugAssert(left_child(), "Need input to generate name");

  if (column_id < _aggregate_expressions.size()) {
    const auto& aggregate_expression = _aggregate_expressions[column_id];

    if (aggregate_expression->alias()) {
      return *aggregate_expression->alias();
    }

    if (left_child()) {
      return aggregate_expression->to_string(left_child()->get_verbose_column_names());
    } else {
      return aggregate_expression->to_string();
    }
  }

  const auto group_by_column_id = column_id - _aggregate_expressions.size();
  DebugAssert(group_by_column_id < _groupby_column_ids.size(), "ColumnID out of range");

  return left_child()->get_verbose_column_name(_groupby_column_ids[group_by_column_id]);
}

void AggregateNode::_on_child_changed() {
  DebugAssert(!right_child(), "AggregateNode can't have a right child.");

  _output_column_names.reset();
}

const std::vector<std::string>& AggregateNode::output_column_names() const {
  Assert(left_child(), "Child not set, can't know output column names without it");
  if (!_output_column_names) {
    _update_output();
  }
  return *_output_column_names;
}

const std::vector<ColumnID>& AggregateNode::output_column_ids_to_input_column_ids() const {
  if (!_output_column_ids_to_input_column_ids) {
    _update_output();
  }
  return *_output_column_ids_to_input_column_ids;
}

std::optional<ColumnID> AggregateNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  DebugAssert(left_child(), "AggregateNode needs a child.");

  auto named_column_reference_without_local_alias = _resolve_local_alias(named_column_reference);
  if (!named_column_reference_without_local_alias) {
    return {};
  }

  /*
   * Search for NamedColumnReference in Aggregate columns ALIASes, if the named_column_reference has no table.
   * These columns are created by the Aggregate Operator, so we have to look through them here.
   */
  std::optional<ColumnID> column_id_aggregate;
  if (!named_column_reference_without_local_alias->table_name) {
    for (auto aggregate_idx = 0u; aggregate_idx < _aggregate_expressions.size(); aggregate_idx++) {
      const auto& aggregate_expression = _aggregate_expressions[aggregate_idx];

      // If AggregateDefinition has no alias, column_name will not match.
      if (named_column_reference_without_local_alias->column_name == aggregate_expression->alias()) {
        // Check that we haven't found a match yet.
        Assert(!column_id_aggregate,
               "Column name " + named_column_reference_without_local_alias->column_name + " is ambiguous.");
        // Aggregate columns come after Group By columns in the Aggregate's output
        column_id_aggregate = ColumnID{static_cast<ColumnID::base_type>(aggregate_idx + _groupby_column_ids.size())};
      }
    }
  }

  /*
   * Search for NamedColumnReference in Group By columns.
   * These columns have been created by another node. Since Aggregates can only have a single child node,
   * we just have to check the left_child for the NamedColumnReference.
   */
  std::optional<ColumnID> column_id_groupby;
  const auto column_id_child =
      left_child()->find_column_id_by_named_column_reference(*named_column_reference_without_local_alias);
  if (column_id_child) {
    const auto iter = std::find(_groupby_column_ids.begin(), _groupby_column_ids.end(), *column_id_child);
    if (iter != _groupby_column_ids.end()) {
      column_id_groupby = ColumnID{static_cast<ColumnID::base_type>(std::distance(_groupby_column_ids.begin(), iter))};
    }
  }

  // Max one can be set, both not being set is fine, as we are in a find_* method
  Assert(!column_id_aggregate || !column_id_groupby,
         "Column name " + named_column_reference_without_local_alias->column_name + " is ambiguous.");

  if (column_id_aggregate) {
    return column_id_aggregate;
  }

  // Optional might not be set.
  return column_id_groupby;
}

ColumnID AggregateNode::get_column_id_for_expression(const std::shared_ptr<Expression>& expression) const {
  const auto column_id = find_column_id_for_expression(expression);
  DebugAssert(column_id, "Expression could not be resolved.");
  return *column_id;
}

std::optional<ColumnID> AggregateNode::find_column_id_for_expression(
    const std::shared_ptr<Expression>& expression) const {
  /**
   * This function does NOT need to check whether an expression is ambiguous.
   * It is only used when translating the HAVING clause.
   * If two expressions are equal, they must refer to the same result.
   * Not checking ambiguity allows perfectly valid queries like:
   *  SELECT a, MAX(b), MAX(b) FROM t GROUP BY a HAVING MAX(b) > 0
   */
  if (expression->type() == ExpressionType::Column) {
    const auto iter = std::find_if(_groupby_column_ids.begin(), _groupby_column_ids.end(),
                                   [&](const auto& rhs) { return expression->column_id() == rhs; });

    if (iter != _groupby_column_ids.end()) {
      const auto idx = std::distance(_groupby_column_ids.begin(), iter);
      return ColumnID{static_cast<ColumnID::base_type>(idx)};
    }
  } else if (expression->type() == ExpressionType::Function) {
    const auto iter = std::find_if(_aggregate_expressions.begin(), _aggregate_expressions.end(), [&](const auto& rhs) {
      DebugAssert(rhs, "Aggregate expressions can not be nullptr!");
      return *expression == *rhs;
    });

    if (iter != _aggregate_expressions.end()) {
      const auto idx = std::distance(_aggregate_expressions.begin(), iter);
      return ColumnID{static_cast<ColumnID::base_type>(idx + _groupby_column_ids.size())};
    }
  } else {
    Fail("Expression type is not supported.");
  }

  // Return unset optional if expression was not found.
  return std::nullopt;
}

std::vector<ColumnID> AggregateNode::get_output_column_ids_for_table(const std::string& table_name) const {
  DebugAssert(left_child(), "AggregateNode needs a child.");

  if (!knows_table(table_name)) {
    return {};
  }

  if (_table_alias && *_table_alias == table_name) {
    return get_output_column_ids();
  }

  const auto input_column_ids_for_table = left_child()->get_output_column_ids_for_table(table_name);

  std::vector<ColumnID> output_column_ids_for_table;

  for (const auto input_column_id : input_column_ids_for_table) {
    const auto iter = std::find(_groupby_column_ids.begin(), _groupby_column_ids.end(), input_column_id);

    if (iter != _groupby_column_ids.end()) {
      const auto index = std::distance(_groupby_column_ids.begin(), iter);
      output_column_ids_for_table.emplace_back(static_cast<ColumnID::base_type>(index));
    }
  }

  return output_column_ids_for_table;
}

void AggregateNode::_update_output() const {
  /**
   * The output (column names and output-to-input mapping) of this node gets cleared whenever a child changed and is
   * re-computed on request. This allows ASTs to be in temporary invalid states (e.g. no left child in Join) and thus
   * allows easier manipulation in the optimizer.
   */

  DebugAssert(!_output_column_ids_to_input_column_ids, "No need to update, _update_output() shouldn't get called.");
  DebugAssert(!_output_column_names, "No need to update, _update_output() shouldn't get called.");
  DebugAssert(left_child(), "Can't set output without input");

  _output_column_names.emplace();
  _output_column_names->reserve(_groupby_column_ids.size() + _aggregate_expressions.size());

  _output_column_ids_to_input_column_ids.emplace();
  _output_column_ids_to_input_column_ids->reserve(_groupby_column_ids.size() + _aggregate_expressions.size());

  /**
   * Set output column ids and names.
   *
   * The Aggregate operator will put all GROUP BY columns in the output table at the beginning,
   * so we first handle those, and afterwards add the column information for the aggregate functions.
   */
  for (const auto groupby_column_id : _groupby_column_ids) {
    _output_column_ids_to_input_column_ids->emplace_back(groupby_column_id);
    _output_column_names->emplace_back(left_child()->output_column_names()[groupby_column_id]);
  }

  for (const auto& aggregate_expression : _aggregate_expressions) {
    DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression must be a function.");

    std::string column_name;

    if (aggregate_expression->alias()) {
      column_name = *aggregate_expression->alias();
    } else {
      /**
       * If the aggregate function has no alias defined in the query, we simply parse the expression back to a string.
       * SQL in the standard does not specify a name to be given.
       * This might result in multiple output columns with the same name, but we accept that.
       * Other DBs behave similarly (e.g. MySQL).
       */
      column_name = aggregate_expression->to_string(left_child()->output_column_names());
    }

    _output_column_names->emplace_back(column_name);
    _output_column_ids_to_input_column_ids->emplace_back(INVALID_COLUMN_ID);
  }
}

}  // namespace opossum
