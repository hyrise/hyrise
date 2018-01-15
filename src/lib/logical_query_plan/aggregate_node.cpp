#include "aggregate_node.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "lqp_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateNode::AggregateNode(const std::vector<std::shared_ptr<LQPExpression>>& aggregate_expressions,
                             const std::vector<LQPColumnReference>& groupby_column_references)
    : AbstractLQPNode(LQPNodeType::Aggregate),
      _aggregate_expressions(aggregate_expressions),
      _groupby_column_references(groupby_column_references) {
  for ([[gnu::unused]] const auto& expression : aggregate_expressions) {
    DebugAssert(expression->type() == ExpressionType::Function, "Aggregate expression must be a function.");
  }
}

std::shared_ptr<AbstractLQPNode> AggregateNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  Assert(left_child(), "Can't clone without child, need it to adapt column references");

  std::vector<std::shared_ptr<LQPExpression>> aggregate_expressions;
  aggregate_expressions.reserve(_aggregate_expressions.size());
  for (const auto& expression : _aggregate_expressions) {
    aggregate_expressions.emplace_back(
        adapt_expression_to_different_lqp(expression->deep_copy(), left_child(), copied_left_child));
  }

  std::vector<LQPColumnReference> groupby_column_references;
  groupby_column_references.reserve(_groupby_column_references.size());
  for (const auto& groupby_column_reference : _groupby_column_references) {
    groupby_column_references.emplace_back(
        adapt_column_reference_to_different_lqp(groupby_column_reference, left_child(), copied_left_child));
  }

  return std::make_shared<AggregateNode>(aggregate_expressions, groupby_column_references);
}

const std::vector<std::shared_ptr<LQPExpression>>& AggregateNode::aggregate_expressions() const {
  return _aggregate_expressions;
}

const std::vector<LQPColumnReference>& AggregateNode::groupby_column_references() const {
  return _groupby_column_references;
}

std::string AggregateNode::description() const {
  std::ostringstream s;

  s << "[Aggregate] ";

  std::vector<std::string> verbose_column_names;
  if (left_child()) {
    verbose_column_names = left_child()->get_verbose_column_names();
  }

  auto stream_aggregate = [&](const std::shared_ptr<LQPExpression>& aggregate_expr) {
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

  if (!_groupby_column_references.empty()) {
    s << " GROUP BY [";
    for (size_t group_by_idx = 0; group_by_idx < _groupby_column_references.size(); ++group_by_idx) {
      s << _groupby_column_references[group_by_idx].description();
      if (group_by_idx + 1 < _groupby_column_references.size()) {
        s << ", ";
      }
    }
    s << "]";
  }

  return s.str();
}

std::string AggregateNode::get_verbose_column_name(ColumnID column_id) const {
  DebugAssert(left_child(), "Need input to generate name");

  if (column_id < _groupby_column_references.size()) {
    return _groupby_column_references[column_id].description();
  }

  const auto aggregate_column_id = column_id - _groupby_column_references.size();
  DebugAssert(aggregate_column_id < _aggregate_expressions.size(), "ColumnID out of range");

  const auto& aggregate_expression = _aggregate_expressions[aggregate_column_id];

  if (aggregate_expression->alias()) {
    return *aggregate_expression->alias();
  }

  if (left_child()) {
    return aggregate_expression->to_string(left_child()->get_verbose_column_names());
  } else {
    return aggregate_expression->to_string();
  }
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

const std::vector<LQPColumnReference>& AggregateNode::output_column_references() const {
  if (!_output_column_references) {
    _update_output();
  }
  return *_output_column_references;
}

LQPColumnReference AggregateNode::get_column_by_expression(const std::shared_ptr<LQPExpression>& expression) const {
  const auto column_id = find_column_by_expression(expression);
  DebugAssert(column_id, "Expression could not be resolved.");
  return *column_id;
}

std::optional<LQPColumnReference> AggregateNode::find_column_by_expression(
    const std::shared_ptr<LQPExpression>& expression) const {
  /**
   * This function does NOT need to check whether an expression is ambiguous.
   * It is only used when translating the HAVING clause.
   * If two expressions are equal, they must refer to the same result.
   * Not checking ambiguity allows perfectly valid queries like:
   *  SELECT a, MAX(b), MAX(b) FROM t GROUP BY a HAVING MAX(b) > 0
   */
  if (expression->type() == ExpressionType::Column) {
    const auto iter = std::find_if(_groupby_column_references.begin(), _groupby_column_references.end(),
                                   [&](const auto& groupby_column_reference) {
                                     return expression->column_reference() == groupby_column_reference;
                                   });

    if (iter != _groupby_column_references.end()) {
      const auto column_id = static_cast<ColumnID>(std::distance(_groupby_column_references.begin(), iter));
      DebugAssert(column_id < output_column_count(), "ColumnID out of range");
      return output_column_references()[column_id];
    }
  } else if (expression->type() == ExpressionType::Function) {
    const auto iter =
        std::find_if(_aggregate_expressions.begin(), _aggregate_expressions.end(), [&](const auto& other) {
          DebugAssert(other, "Aggregate expressions can not be nullptr!");
          return *expression == *other;
        });

    if (iter != _aggregate_expressions.end()) {
      const auto idx = std::distance(_aggregate_expressions.begin(), iter);
      return LQPColumnReference{shared_from_this(), static_cast<ColumnID>(idx + _groupby_column_references.size())};
    }
  }

  return std::nullopt;
}

void AggregateNode::_update_output() const {
  /**
   * The output (column names and output-to-input mapping) of this node gets cleared whenever a child changed and is
   * re-computed on request. This allows LQPs to be in temporary invalid states (e.g. no left child in Join) and thus
   * allows easier manipulation in the optimizer.
   */

  DebugAssert(!_output_column_references, "No need to update, _update_output() shouldn't get called.");
  DebugAssert(!_output_column_names, "No need to update, _update_output() shouldn't get called.");
  DebugAssert(left_child(), "Can't set output without input");

  _output_column_names.emplace();
  _output_column_names->reserve(_groupby_column_references.size() + _aggregate_expressions.size());

  _output_column_references.emplace();
  _output_column_references->reserve(_groupby_column_references.size() + _aggregate_expressions.size());

  /**
   * Set output column ids and names.
   *
   * The Aggregate operator will put all GROUP BY columns in the output table at the beginning,
   * so we first handle those, and afterwards add the column information for the aggregate functions.
   */
  for (const auto& groupby_column_reference : _groupby_column_references) {
    _output_column_references->emplace_back(groupby_column_reference);

    const auto input_column_id = left_child()->get_output_column_id(groupby_column_reference);
    _output_column_names->emplace_back(left_child()->output_column_names()[input_column_id]);
  }

  auto column_id = static_cast<ColumnID>(_groupby_column_references.size());
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
    _output_column_references->emplace_back(shared_from_this(), column_id);
    column_id++;
  }
}

}  // namespace opossum
