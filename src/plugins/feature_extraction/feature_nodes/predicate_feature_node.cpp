#include "predicate_feature_node.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace {

using namespace opossum;  // NOLINT

std::shared_ptr<AbstractExpression> resolve_uncorrelated_subqueries(
    const std::shared_ptr<AbstractExpression>& predicate) {
  if (!std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate) &&
      !std::dynamic_pointer_cast<IsNullExpression>(predicate) &&
      !std::dynamic_pointer_cast<BetweenExpression>(predicate)) {
    // We have no dedicated Impl for these, so we leave them untouched
    return predicate;
  }

  auto arguments_count = predicate->arguments.size();
  auto new_arguments = std::vector<std::shared_ptr<AbstractExpression>>();
  new_arguments.reserve(arguments_count);
  auto computed_subqueries_count = int{0};

  for (auto argument_idx = size_t{0}; argument_idx < arguments_count; ++argument_idx) {
    const auto subquery = std::dynamic_pointer_cast<PQPSubqueryExpression>(predicate->arguments.at(argument_idx));
    if (!subquery || subquery->is_correlated()) {
      new_arguments.emplace_back(predicate->arguments.at(argument_idx)->deep_copy());
      continue;
    }

    new_arguments.emplace_back(std::make_shared<ValueExpression>(int32_t{0}));
  }
  // Return original predicate if we did not compute any subquery results
  if (computed_subqueries_count == 0) {
    return predicate;
  }

  /**
   * (2) Create new predicate with arguments from step (1)
   */
  if (auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate)) {
    auto left_operand = new_arguments.at(0);
    auto right_operand = new_arguments.at(1);
    DebugAssert(left_operand && right_operand, "Unexpected null pointer.");
    return std::make_shared<BinaryPredicateExpression>(binary_predicate->predicate_condition, left_operand,
                                                       right_operand);
  }
  if (auto between_predicate = std::dynamic_pointer_cast<BetweenExpression>(predicate)) {
    auto value = new_arguments.at(0);
    auto lower_bound = new_arguments.at(1);
    auto upper_bound = new_arguments.at(2);
    DebugAssert(value && lower_bound && upper_bound, "Unexpected null pointer.");
    return std::make_shared<BetweenExpression>(between_predicate->predicate_condition, value, lower_bound, upper_bound);
  }
  if (auto is_null_predicate = std::dynamic_pointer_cast<IsNullExpression>(predicate)) {
    auto operand = new_arguments.at(0);
    DebugAssert(operand, "Unexpected null pointer.");
    return std::make_shared<IsNullExpression>(is_null_predicate->predicate_condition, operand);
  }

  Fail("Unexpected predicate type");
}

}  // namespace

namespace opossum {

PredicateFeatureNode::PredicateFeatureNode(const std::shared_ptr<AbstractExpression>& lqp_expression,
                                           const std::shared_ptr<AbstractExpression>& pqp_expression,
                                           const std::shared_ptr<AbstractFeatureNode>& operator_node)
    : AbstractFeatureNode(FeatureNodeType::Predicate, nullptr, nullptr) {
  Assert(pqp_expression->type == ExpressionType::Predicate, "Predicate expected");
  const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(pqp_expression);
  _predicate_condition = predicate_expression->predicate_condition;

  const auto resolved_predicate = resolve_uncorrelated_subqueries(pqp_expression);
  if (const auto binary_predicate_expression =
          std::dynamic_pointer_cast<BinaryPredicateExpression>(resolved_predicate)) {
    const auto left_operand = binary_predicate_expression->left_operand();
    const auto right_operand = binary_predicate_expression->right_operand();

    const auto left_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(left_operand);
    const auto right_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(right_operand);

    auto left_value = expression_get_value_or_parameter(*left_operand);
    auto right_value = expression_get_value_or_parameter(*right_operand);

    if (left_column_expression && right_column_expression) {
      _column_vs_column = true;
      _left_input = ColumnFeatureNode::from_expression(operator_node->left_input(), lqp_expression->arguments[0],
                                                       left_column_expression->column_id);
      _right_input = ColumnFeatureNode::from_expression(operator_node->left_input(), lqp_expression->arguments[1],
                                                        right_column_expression->column_id);
      return;
    }

    if (_predicate_condition == PredicateCondition::Like || _predicate_condition == PredicateCondition::NotLike) {
      Assert((left_column_expression && !right_column_expression), "expected only one column for like");
      _left_input = ColumnFeatureNode::from_expression(operator_node->left_input(), lqp_expression->arguments[0],
                                                       left_column_expression->column_id);
      return;
    }

    if ((left_column_expression && right_value) || (right_column_expression && left_value)) {
      const auto left_is_column = left_column_expression ? true : false;
      const auto& column_expression = left_is_column ? left_column_expression : right_column_expression;
      const auto& lqp_column = left_is_column ? lqp_expression->arguments[0] : lqp_expression->arguments[1];
      _column_vs_value = true;
      _left_input =
          ColumnFeatureNode::from_expression(operator_node->left_input(), lqp_column, column_expression->column_id);
      return;
    }
  }

  if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(resolved_predicate)) {
    _column_vs_value = true;
    Assert(is_null_expression->operand()->type == ExpressionType::PQPColumn, "Expected column");
    const auto& pqp_column = static_cast<PQPColumnExpression&>(*is_null_expression->operand());
    _left_input = ColumnFeatureNode::from_expression(operator_node->left_input(), lqp_expression->arguments[0],
                                                     pqp_column.column_id);
    return;
  }

  if (const auto between_expression = std::dynamic_pointer_cast<BetweenExpression>(resolved_predicate)) {
    const auto left_column = std::dynamic_pointer_cast<PQPColumnExpression>(between_expression->value());
    auto lower_bound_value = expression_get_value_or_parameter(*between_expression->lower_bound());
    auto upper_bound_value = expression_get_value_or_parameter(*between_expression->upper_bound());
    if (left_column && lower_bound_value && upper_bound_value) {
      _left_input = ColumnFeatureNode::from_expression(operator_node->left_input(), lqp_expression->arguments[0],
                                                       left_column->column_id);
      return;
    }
  }

  const auto& arguments = pqp_expression->arguments;
  const auto argument_count = arguments.size();
  auto columns = std::vector<std::shared_ptr<AbstractFeatureNode>>{};
  for (auto argument_idx = ColumnID{0}; argument_idx < argument_count; ++argument_idx) {
    if (auto pqp_column = std::dynamic_pointer_cast<PQPColumnExpression>(arguments[argument_idx])) {
      Assert(!_left_input, "Input column already set");
      _left_input = ColumnFeatureNode::from_expression(operator_node->left_input(),
                                                       lqp_expression->arguments[argument_idx], pqp_column->column_id);
    }
  }

  Assert(_left_input, "Scan should operate on a column");
  _is_complex = true;
}

std::shared_ptr<FeatureVector> PredicateFeatureNode::_on_to_feature_vector() const {
  return one_hot_encoding<PredicateCondition>(_predicate_condition);
}

const std::vector<std::string>& PredicateFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& PredicateFeatureNode::headers() {
  static const auto ohe_headers_condition = one_hot_headers<PredicateCondition>("condition.");
  return ohe_headers_condition;
}

}  // namespace opossum
