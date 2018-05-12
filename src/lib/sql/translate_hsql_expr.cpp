#include "translate_hsql_expr.hpp"

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/not_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/value_placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "sql/sql_translator.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& expr,
                                                        const std::shared_ptr<SQLIdentifierContext>& sql_identifier_context,
                                                        const UseMvcc use_mvcc);

}  // namespace opossum

namespace {

using namespace opossum; // NOLINT

const std::unordered_map<hsql::OperatorType, ArithmeticOperator> hsql_arithmetic_operators = {
  {hsql::kOpPlus, ArithmeticOperator::Addition},
  {hsql::kOpMinus, ArithmeticOperator::Subtraction},
  {hsql::kOpAsterisk, ArithmeticOperator::Multiplication},
  {hsql::kOpSlash, ArithmeticOperator::Division},
  {hsql::kOpPercentage, ArithmeticOperator::Modulo},
  {hsql::kOpCaret, ArithmeticOperator::Power},
};

const std::unordered_map<hsql::OperatorType, LogicalOperator> hsql_logical_operators = {
  {hsql::kOpAnd, LogicalOperator::And},
  {hsql::kOpOr, LogicalOperator::Or}
};

const std::unordered_map<hsql::OperatorType, PredicateCondition> hsql_predicate_condition = {
  {hsql::kOpBetween, PredicateCondition::Between},
  {hsql::kOpEquals, PredicateCondition::Equals},
  {hsql::kOpNotEquals, PredicateCondition::NotEquals},
  {hsql::kOpLess, PredicateCondition::LessThan},
  {hsql::kOpLessEq, PredicateCondition::LessThanEquals},
  {hsql::kOpGreater, PredicateCondition::GreaterThan},
  {hsql::kOpGreaterEq, PredicateCondition::GreaterThanEquals},
  {hsql::kOpLike, PredicateCondition::Like},
  {hsql::kOpNotLike, PredicateCondition::NotLike},
  {hsql::kOpIsNull, PredicateCondition::IsNull}
};

std::shared_ptr<AbstractExpression> translate_hsql_case(const hsql::Expr& expr,
                                                        const std::shared_ptr<SQLIdentifierContext>& sql_identifier_context,
                                                        const UseMvcc use_mvcc) {
  /**
   * There is a "simple" and a "searched" CASE syntax, see http://www.oratable.com/simple-case-searched-case/
   * Hyrise supports both.
   */

  Assert(expr.exprList, "Unexpected SQLParserResult. Case needs exprList");
  Assert(!expr.exprList->empty(), "Unexpected SQLParserResult. Case needs non-empty exprList");

  // "a + b" in "CASE a + b WHEN ... THEN ... END", or nullptr when using the "searched" CASE syntax
  auto simple_case_left_operand = std::shared_ptr<AbstractExpression>{};
  if (expr.expr) simple_case_left_operand = translate_hsql_expr(*expr.expr, sql_identifier_context, use_mvcc);

  // Initialize CASE with the ELSE expression and then put the remaining WHEN...THEN... clauses on top of that
  // in reverse order
  auto current_case_expression = std::shared_ptr<AbstractExpression>{};
  if (expr.expr2) {
    current_case_expression = translate_hsql_expr(*expr.expr2, sql_identifier_context, use_mvcc);
  } else {
    // No ELSE specified, use NULL
    current_case_expression = std::make_shared<ValueExpression>(NullValue{});
  }

  for (auto case_reverse_idx = size_t{0}; case_reverse_idx < expr.exprList->size(); ++case_reverse_idx) {
    const auto case_idx = expr.exprList->size() - case_reverse_idx - 1;
    const auto case_clause = (*expr.exprList)[case_idx];

    auto when = translate_hsql_expr(*case_clause->expr, sql_identifier_context, use_mvcc);
    if (simple_case_left_operand) {
      when = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, simple_case_left_operand, when);
    }

    const auto then = translate_hsql_expr(*case_clause->expr2, sql_identifier_context, use_mvcc);
    current_case_expression = std::make_shared<CaseExpression>(when, then, current_case_expression);
  }

  return current_case_expression;
}
} // namespace

namespace opossum {

std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& expr,
                                                        const std::shared_ptr<SQLIdentifierContext>& sql_identifier_context,
                                                        const UseMvcc use_mvcc) {
  auto name = expr.name != nullptr ? std::string(expr.name) : "";

  std::shared_ptr<AbstractExpression> left;
  std::shared_ptr<AbstractExpression> right;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;

  // TODO fix case parsing
  if (!(expr.type == hsql::kExprOperator && expr.opType == hsql::kOpCase)) {
    if (expr.exprList) {
      arguments.reserve(expr.exprList->size());
      for (const auto *hsql_argument : *(expr.exprList)) {
        arguments.emplace_back(translate_hsql_expr(*hsql_argument, sql_identifier_context, use_mvcc));
      }
    }
  }

  if (expr.expr) left = translate_hsql_expr(*expr.expr, sql_identifier_context, use_mvcc);
  if (expr.expr2) right = translate_hsql_expr(*expr.expr2, sql_identifier_context, use_mvcc);

  switch (expr.type) {
    case hsql::kExprColumnRef: {
      const auto table_name = expr.table ? std::optional<std::string>(std::string(expr.table)) : std::nullopt;
      const auto identifier = SQLIdentifier{name, table_name};

      return sql_identifier_context->resolve_identifier_strict(identifier);
    }

    case hsql::kExprLiteralFloat:
      return std::make_shared<ValueExpression>(expr.fval);

    case hsql::kExprLiteralString:
      Assert(expr.name, "No value given for string literal");
      return std::make_shared<ValueExpression>(name);

    case hsql::kExprLiteralInt:
      if (static_cast<int32_t>(expr.ival) == expr.ival) {
        return std::make_shared<ValueExpression>(static_cast<int32_t>(expr.ival));
      } else {
        return std::make_shared<ValueExpression>(expr.ival);
      }

    case hsql::kExprLiteralNull:
      return std::make_shared<ValueExpression>(NullValue{});

    case hsql::kExprParameter:
      return std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{static_cast<uint16_t>(expr.ival)});

    case hsql::kExprFunctionRef: {
      Assert(expr.exprList, "FunctionRef has no exprList. Bug in sqlparser?");

      // convert to upper-case to find mapping
      std::transform(name.begin(), name.end(), name.begin(), [](const auto c) { return std::toupper(c); });

      const auto aggregate_iter = aggregate_function_to_string.right.find(name);
      if (aggregate_iter != aggregate_function_to_string.right.end()) {
        auto aggregate_function = aggregate_iter->second;

        if (aggregate_function == AggregateFunction::Count && expr.distinct) {
          aggregate_function = AggregateFunction::CountDistinct;
        }

        switch (aggregate_function) {
          case AggregateFunction::Min: case AggregateFunction::Max: case AggregateFunction::Sum:
          case AggregateFunction::Avg:
            Assert(arguments.size() == 1, "Expected exactly one argument for this AggregateFunction");
            return std::make_shared<AggregateExpression>(aggregate_function, arguments[0]);

          case AggregateFunction::Count: case AggregateFunction::CountDistinct:
            return std::make_shared<AggregateExpression>(aggregate_function);
        }

      } else {
//        const auto function_iter = function_type_to_string.right.find(name);

//        if (function_iter != function_type_to_string.right.end()) {
//          return std::make_shared<FunctionExpression>(function_iter->second, arguments);
//        } else {
          Fail(std::string{"Couldn't resolve function '"} + name + "'");
//        }
      }
    }

    case hsql::kExprOperator: {
      // Translate ArithmeticExpression
      const auto arithmetic_operators_iter = hsql_arithmetic_operators.find(expr.opType);
      if (arithmetic_operators_iter != hsql_arithmetic_operators.end()) {
        Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary expression.");
        return std::make_shared<ArithmeticExpression>(arithmetic_operators_iter->second, left, right);
      }

      // Translate PredicateExpression
      const auto predicate_condition_iter = hsql_predicate_condition.find(expr.opType);
      if (predicate_condition_iter != hsql_predicate_condition.end()) {
        const auto predicate_condition = predicate_condition_iter->second;

        if (is_ordering_predicate_condition(predicate_condition)) {
          Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary_expression");
          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
        } else if (predicate_condition == PredicateCondition::Between) {
          Assert(arguments.size() == 2, "Expected two arguments for BETWEEN");
          return std::make_shared<BetweenExpression>(left, arguments[0], arguments[1]);
        }
      }

      switch (expr.opType) {
        case hsql::kOpCase: return translate_hsql_case(expr, sql_identifier_context, use_mvcc);
        case hsql::kOpOr: return std::make_shared<LogicalExpression>(LogicalOperator::Or, left, right);
        case hsql::kOpAnd: return std::make_shared<LogicalExpression>(LogicalOperator::And, left, right);

        default:
          Fail("Not handling this OperatorType yet");
      }

//      const auto predicate_iter = hsql_predicate_condition.find(expr.opType);
//      if (predicate_iter != hsql_predicate_condition.end()) {
//        const auto predicate_condition = predicate_iter->second;

//        if (predicate_condition == PredicateCondition::Between) {
//          Assert(left && !right, "Illegal arguments for BETWEEN. Bug in sqlparser?");
//          Assert(expr.exprList && (*expr.exprList)[0] && (*expr.exprList)[1], "Illegal arguments for BETWEEN. Bug in sqlparser?");

//          const auto lower_bound = translate_hsql_expr(*(*expr.exprList)[0], translation_state);
//          const auto upper_bound = translate_hsql_expr(*(*expr.exprList)[1], translation_state);

//          return std::make_shared<BetweenExpression>(left, lower_bound, upper_bound);
//        } else if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
//          Assert(left && !right, "Illegal arguments for IsNull/IsNotNull. Bug in sqlparser?");
//          return std::make_shared<IsNullExpression>(predicate_condition, left);
//        } else {
//          Assert(left && right, "Illegal arguments for binary predicate. Bug in sqlparser?");
//          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
//        }
//      }

//      const auto logical_iter = hsql_logical_operators.find(expr.opType);
//      if (logical_iter != hsql_logical_operators.end()) {
//        Assert(left && right, "Wrong number of arguments. Bug in sqlparser?");
//        return std::make_shared<LogicalExpression>(logical_iter->second, left, right);
//      }

//      if (expr.opType == hsql::kOpNot){
//        Assert(left && !right, "Wrong number of arguments. Bug in sqlparser?");
//        return std::make_shared<NotExpression>(left);
//      }

//      Fail("Unsupported expression type");
    }

    case hsql::kExprSelect: {
      Fail("Nyi");
//      auto external_column_identifier_proxy = translation_state->create_external_column_identifier_proxy();
//
//      const auto lqp = SQLTranslator{validate}.translate_select_statement(*expr.select, external_column_identifier_proxy);
//
//      return std::make_shared<LQPSelectExpression>(lqp, external_column_identifier_proxy->referenced_external_expressions());
    }

    case hsql::kExprArray:
      Fail("Nyi");
      //return std::make_shared<ArrayExpression>(arguments);

    case hsql::kExprHint:
    case hsql::kExprStar:
    case hsql::kExprArrayIndex:
      Fail("Can't translate this hsql expression into a Hyrise expression");

    default:
      Fail("Nyie");
  }
}
}  // namespace opossum
