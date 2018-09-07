#include "expression_utils.hpp"

#include <algorithm>
#include <queue>
#include <sstream>

#include "logical_expression.hpp"
#include "lqp_column_expression.hpp"
#include "lqp_select_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "pqp_select_expression.hpp"

namespace opossum {

bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  return std::equal(expressions_a.begin(), expressions_a.end(), expressions_b.begin(), expressions_b.end(),
                    [&](const auto& expression_a, const auto& expression_b) { return *expression_a == *expression_b; });
}

bool expressions_equal_to_expressions_in_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_left,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_right, const LQPNodeMapping& node_mapping) {
  if (expressions_left.size() != expressions_right.size()) return false;

  for (auto expression_idx = size_t{0}; expression_idx < expressions_left.size(); ++expression_idx) {
    const auto& expression_left = *expressions_left[expression_idx];
    const auto& expression_right = *expressions_right[expression_idx];

    if (!expression_equal_to_expression_in_different_lqp(expression_left, expression_right, node_mapping)) return false;
  }

  return true;
}

bool expression_equal_to_expression_in_different_lqp(const AbstractExpression& expression_left,
                                                     const AbstractExpression& expression_right,
                                                     const LQPNodeMapping& node_mapping) {
  /**
   * Compare expression_left to expression_right by creating a deep copy of expression_left and adapting it to the LQP
   * of expression_right, then perform a normal comparison of two expressions in the same LQP.
   */

  auto copied_expression_left = expression_left.deep_copy();
  expression_adapt_to_different_lqp(copied_expression_left, node_mapping);
  return *copied_expression_left == expression_right;
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_deep_copy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());
  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression->deep_copy());
  }
  return copied_expressions;
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LQPNodeMapping& node_mapping) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());

  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression_copy_and_adapt_to_different_lqp(*expression, node_mapping));
  }

  return copied_expressions;
}

std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(const AbstractExpression& expression,
                                                                               const LQPNodeMapping& node_mapping) {
  auto copied_expression = expression.deep_copy();
  expression_adapt_to_different_lqp(copied_expression, node_mapping);
  return copied_expression;
}

void expression_adapt_to_different_lqp(std::shared_ptr<AbstractExpression>& expression,
                                       const LQPNodeMapping& node_mapping) {
  visit_expression(expression, [&](auto& expression_ptr) {
    if (expression_ptr->type != ExpressionType::LQPColumn) return ExpressionVisitation::VisitArguments;

    const auto lqp_column_expression_ptr = std::dynamic_pointer_cast<LQPColumnExpression>(expression_ptr);
    Assert(lqp_column_expression_ptr, "Asked to adapt expression in LQP, but encountered non-LQP ColumnExpression");

    expression_ptr = expression_adapt_to_different_lqp(*lqp_column_expression_ptr, node_mapping);

    return ExpressionVisitation::DoNotVisitArguments;
  });
}

std::shared_ptr<LQPColumnExpression> expression_adapt_to_different_lqp(const LQPColumnExpression& lqp_column_expression,
                                                                       const LQPNodeMapping& node_mapping) {
  const auto node = lqp_column_expression.column_reference.original_node();
  const auto node_mapping_iter = node_mapping.find(node);
  Assert(node_mapping_iter != node_mapping.end(),
         "Couldn't find referenced node (" + node->description() + ") in NodeMapping");

  LQPColumnReference adapted_column_reference{node_mapping_iter->second,
                                              lqp_column_expression.column_reference.original_column_id()};

  return std::make_shared<LQPColumnExpression>(adapted_column_reference);
}

std::string expression_column_names(const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::stringstream stream;

  if (!expressions.empty()) stream << expressions.front()->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < expressions.size(); ++expression_idx) {
    stream << ", " << expressions[expression_idx]->as_column_name();
  }

  return stream.str();
}

DataType expression_common_type(const DataType lhs, const DataType rhs) {
  Assert(lhs != DataType::Null || rhs != DataType::Null, "Can't deduce common type if both sides are NULL");
  Assert((lhs == DataType::String) == (rhs == DataType::String), "Strings only compatible with strings");

  // Long+NULL -> Long; NULL+Long -> Long; NULL+NULL -> NULL
  if (lhs == DataType::Null) return rhs;
  if (rhs == DataType::Null) return lhs;

  if (lhs == DataType::String) return DataType::String;

  if (lhs == DataType::Double || rhs == DataType::Double) return DataType::Double;
  if (lhs == DataType::Long) {
    return is_floating_point_data_type(rhs) ? DataType::Double : DataType::Long;
  }
  if (rhs == DataType::Long) {
    return is_floating_point_data_type(lhs) ? DataType::Double : DataType::Long;
  }
  if (lhs == DataType::Float || rhs == DataType::Float) return DataType::Float;

  return DataType::Int;
}

bool expression_evaluable_on_lqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLQPNode& lqp) {
  auto evaluable = true;

  visit_expression(expression, [&](const auto& sub_expression) {
    if (lqp.find_column_id(*sub_expression)) return ExpressionVisitation::DoNotVisitArguments;
    if (sub_expression->type == ExpressionType::LQPColumn) evaluable = false;
    return ExpressionVisitation::VisitArguments;
  });

  return evaluable;
}

std::vector<std::shared_ptr<AbstractExpression>> expression_flatten_conjunction(
    const std::shared_ptr<AbstractExpression>& expression) {
  std::vector<std::shared_ptr<AbstractExpression>> flattened_expressions;

  visit_expression(expression, [&](const auto& sub_expression) {
    if (sub_expression->type == ExpressionType::Logical) {
      const auto logical_expression = std::static_pointer_cast<LogicalExpression>(sub_expression);
      if (logical_expression->logical_operator == LogicalOperator::And) return ExpressionVisitation::VisitArguments;
    }
    flattened_expressions.emplace_back(sub_expression);
    return ExpressionVisitation::DoNotVisitArguments;
  });

  return flattened_expressions;
}

void expression_set_parameters(const std::shared_ptr<AbstractExpression>& expression,
                               const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  visit_expression(expression, [&](auto& sub_expression) {
    if (sub_expression->type == ExpressionType::Parameter) {
      auto parameter_expression = std::static_pointer_cast<ParameterExpression>(sub_expression);
      const auto value_iter = parameters.find(parameter_expression->parameter_id);
      if (value_iter != parameters.end()) {
        parameter_expression->set_value(value_iter->second);
      }
      return ExpressionVisitation::DoNotVisitArguments;

    } else if (const auto pqp_select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(sub_expression);
               pqp_select_expression) {
      pqp_select_expression->pqp->set_parameters(parameters);
      return ExpressionVisitation::DoNotVisitArguments;

    } else {
      return ExpressionVisitation::VisitArguments;
    }
  });
}

void expressions_set_parameters(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  for (const auto& expression : expressions) {
    expression_set_parameters(expression, parameters);
  }
}

void expression_set_transaction_context(const std::shared_ptr<AbstractExpression>& expression,
                                        const std::weak_ptr<TransactionContext>& transaction_context) {
  visit_expression(expression, [&](auto& sub_expression) {
    if (sub_expression->type != ExpressionType::PQPSelect) return ExpressionVisitation::VisitArguments;

    const auto pqp_select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(sub_expression);
    Assert(pqp_select_expression, "Expected a PQPSelectExpression here")
        pqp_select_expression->pqp->set_transaction_context_recursively(transaction_context);

    return ExpressionVisitation::DoNotVisitArguments;
  });
}

void expressions_set_transaction_context(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                         const std::weak_ptr<TransactionContext>& transaction_context) {
  for (const auto& expression : expressions) {
    expression_set_transaction_context(expression, transaction_context);
  }
}

bool expression_contains_placeholders(const std::shared_ptr<AbstractExpression>& expression) {
  auto placeholder_found = false;

  visit_expression(expression, [&](const auto& sub_expression) {
    const auto parameter_expression = std::dynamic_pointer_cast<ParameterExpression>(sub_expression);
    if (parameter_expression) {
      placeholder_found |= parameter_expression->parameter_expression_type == ParameterExpressionType::ValuePlaceholder;
    }

    return ExpressionVisitation::VisitArguments;
  });

  return placeholder_found;
}

}  // namespace opossum
