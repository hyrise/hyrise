#include "aggregate_node.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateNode::AggregateNode(const std::vector<std::shared_ptr<AbstractExpression>>& group_by_expressions,
                             const std::vector<std::shared_ptr<AbstractExpression>>& aggregate_expressions)
    : AbstractLQPNode(LQPNodeType::Aggregate, {/* Expressions added below*/}),
      aggregate_expressions_begin_idx{group_by_expressions.size()} {
#if HYRISE_DEBUG
  for (const auto& aggregate_expression : aggregate_expressions) {
    DebugAssert(aggregate_expression->type == ExpressionType::Aggregate,
                "Expression used as aggregate expression must be of type AggregateExpression.");
  }
#endif

  node_expressions.resize(group_by_expressions.size() + aggregate_expressions.size());
  std::copy(group_by_expressions.begin(), group_by_expressions.end(), node_expressions.begin());
  std::copy(aggregate_expressions.begin(), aggregate_expressions.end(),
            node_expressions.begin() + group_by_expressions.size());
}

std::string AggregateNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);
  std::stringstream stream;

  stream << "[Aggregate] ";

  stream << "GroupBy: [";
  for (auto expression_idx = size_t{0}; expression_idx < aggregate_expressions_begin_idx; ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode);
    if (expression_idx + 1 < aggregate_expressions_begin_idx) stream << ", ";
  }
  stream << "] ";

  stream << "Aggregates: [";
  for (auto expression_idx = aggregate_expressions_begin_idx; expression_idx < node_expressions.size();
       ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode);
    if (expression_idx + 1 < node_expressions.size()) stream << ", ";
  }
  stream << "]";

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& AggregateNode::column_expressions() const {
  // We do not return node_expressions directly here, because we do not want to expose ANY() to the following LQP
  // nodes. This way, we execute ANY() as intended, but do not have to traverse the LQP upwards and adapt nodes
  // that reference the ANY'd column.
  _column_expressions.resize(node_expressions.size());
  std::copy(node_expressions.begin(), node_expressions.end(), _column_expressions.begin());

  for (auto expression_idx = aggregate_expressions_begin_idx; expression_idx < _column_expressions.size();
       ++expression_idx) {
    auto& column_expression = _column_expressions[expression_idx];
    DebugAssert(column_expression->type == ExpressionType::Aggregate,
                "Unexpected non-aggregate in list of aggregates.");
    if (column_expression->type == ExpressionType::Aggregate) {
      const auto& aggregate_expression = static_cast<AggregateExpression&>(*column_expression);
      if (aggregate_expression.aggregate_function == AggregateFunction::Any) {
        column_expression = column_expression->arguments[0];
      }
    }
  }

  return _column_expressions;
}

bool AggregateNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < node_expressions.size(), "ColumnID out of range");
  Assert(left_input(), "Need left input to determine nullability");
  return node_expressions[column_id]->is_nullable_on_lqp(*left_input());
}

std::shared_ptr<AbstractLQPNode> AggregateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{
      node_expressions.begin(), node_expressions.begin() + aggregate_expressions_begin_idx};

  const auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{
      node_expressions.begin() + aggregate_expressions_begin_idx, node_expressions.end()};

  return std::make_shared<AggregateNode>(
      expressions_copy_and_adapt_to_different_lqp(group_by_expressions, node_mapping),
      expressions_copy_and_adapt_to_different_lqp(aggregate_expressions, node_mapping));
}

size_t AggregateNode::_on_shallow_hash() const { return aggregate_expressions_begin_idx; }

bool AggregateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& aggregate_node = static_cast<const AggregateNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(node_expressions, aggregate_node.node_expressions,
                                                           node_mapping) &&
         aggregate_expressions_begin_idx == aggregate_node.aggregate_expressions_begin_idx;
}
}  // namespace opossum
