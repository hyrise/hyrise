#include "operator_scan_predicate.hpp"

#include "constant_mappings.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/parameter_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace {

using namespace opossum;                         // NOLINT
using namespace opossum::expression_functional;  // NOLINT

std::optional<AllParameterVariant> resolve_all_parameter_variant(const AbstractExpression& expression,
                                                                 const AbstractLQPNode& node) {
  auto value = AllParameterVariant{};

  if (const auto* value_expression = dynamic_cast<const ValueExpression*>(&expression)) {
    value = value_expression->value;
  } else if (const auto column_id = node.find_column_id(expression)) {
    value = *column_id;
  } else if (const auto parameter_expression = dynamic_cast<const ParameterExpression*>(&expression)) {
    value = parameter_expression->parameter_id;
  } else {
    return std::nullopt;
  }

  return value;
}

}  // namespace

namespace opossum {

std::string OperatorScanPredicate::to_string(const std::shared_ptr<const Table>& table) const {
  std::string column_name_left = std::string("Column #") + std::to_string(column_id);
  if (table) {
    column_name_left = table->column_name(column_id);
  }

  std::string right = opossum::to_string(value);
  if (table && is_column_id(value)) {
    right = table->column_name(boost::get<ColumnID>(value));
  }

  std::stringstream stream;
  stream << column_name_left << " " << predicate_condition_to_string.left.at(predicate_condition) << " " << right;

  if (predicate_condition == PredicateCondition::Between) {
    stream << " AND " << *value2;
  }

  return stream.str();
}

std::optional<std::vector<OperatorScanPredicate>> OperatorScanPredicate::from_expression(
    const AbstractExpression& expression, const AbstractLQPNode& node) {
  const auto* predicate = dynamic_cast<const AbstractPredicateExpression*>(&expression);
  if (!predicate) return std::nullopt;

  Assert(!predicate->arguments.empty(), "Expect PredicateExpression to have one or more arguments");

  auto predicate_condition = predicate->predicate_condition;

  auto argument_a = resolve_all_parameter_variant(*predicate->arguments[0], node);
  if (!argument_a) return std::nullopt;

  if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    if (is_column_id(*argument_a)) {
      return std::vector<OperatorScanPredicate>{
          OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition}};
    } else {
      return std::nullopt;
    }
  }

  Assert(predicate->arguments.size() > 1, "Expect non-unary PredicateExpression to have two or more arguments");

  auto argument_b = resolve_all_parameter_variant(*expression.arguments[1], node);
  if (!argument_b) return std::nullopt;

  // We can handle x BETWEEN a AND b if a and b are scalar values of the same data type. Otherwise, the condition gets
  // translated into two scans. Theoretically, we could also implement all variations where x, a and b are
  // non-scalar and of varying types, but as these are used less frequently, would require more code, and increase
  // compile time, we don't do that for now.
  if (predicate_condition == PredicateCondition::Between) {
    Assert(predicate->arguments.size() == 3, "Expect ternary PredicateExpression to have three arguments");

    auto argument_c = resolve_all_parameter_variant(*expression.arguments[2], node);
    if (!argument_c) return std::nullopt;

    if (is_column_id(*argument_a) && is_variant(*argument_b) && is_variant(*argument_c) &&
        predicate->arguments[1]->data_type() == predicate->arguments[2]->data_type() &&
        !variant_is_null(boost::get<AllTypeVariant>(*argument_b)) &&
        !variant_is_null(boost::get<AllTypeVariant>(*argument_c))) {
      // This is the BETWEEN case that we can handle
      return std::vector<OperatorScanPredicate>{
          OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition, *argument_b, *argument_c}};
    }

    PerformanceWarning("BETWEEN handled as two table scans because no BETWEEN specialization was available");

    // We can't handle the case, so we translate it into two predicates
    auto lower_bound_predicates =
        from_expression(*greater_than_equals_(predicate->arguments[0], predicate->arguments[1]), node);
    auto upper_bound_predicates =
        from_expression(*less_than_equals_(predicate->arguments[0], predicate->arguments[2]), node);

    if (!lower_bound_predicates || !upper_bound_predicates) return std::nullopt;

    auto predicates = *lower_bound_predicates;
    predicates.insert(predicates.end(), upper_bound_predicates->begin(), upper_bound_predicates->end());

    return predicates;
  }

  if (!is_column_id(*argument_a)) {
    if (is_column_id(*argument_b)) {
      std::swap(argument_a, argument_b);
      predicate_condition = flip_predicate_condition(predicate_condition);
    } else {
      // Literal-only predicates like "5 > 3" cannot be turned into OperatorScanPredicates
      return std::nullopt;
    }
  }

  return std::vector<OperatorScanPredicate>{
      OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition, *argument_b}};
}

OperatorScanPredicate::OperatorScanPredicate(const ColumnID column_id, const PredicateCondition predicate_condition,
                                             const AllParameterVariant& value,
                                             const std::optional<AllParameterVariant>& value2)
    : column_id(column_id), predicate_condition(predicate_condition), value(value), value2(value2) {}

}  // namespace opossum
