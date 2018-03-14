#include "hsql_expr_translator.hpp"

#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

std::shared_ptr<LQPExpression> HSQLExprTranslator::to_lqp_expression(
    const hsql::Expr& expr, const std::shared_ptr<AbstractLQPNode>& input_node) {
  auto name = expr.name != nullptr ? std::string(expr.name) : "";
  auto alias = expr.alias != nullptr ? std::optional<std::string>(expr.alias) : std::nullopt;

  std::shared_ptr<LQPExpression> node;
  std::shared_ptr<LQPExpression> left;
  std::shared_ptr<LQPExpression> right;

  if (expr.expr != nullptr) {
    left = to_lqp_expression(*expr.expr, input_node);
  }

  if (expr.expr2 != nullptr) {
    right = to_lqp_expression(*expr.expr2, input_node);
  }

  // Since subselects need recursive translation, they are handled in SQLTranslator to avoid circular inclusion
  DebugAssert(expr.type != hsql::kExprSelect, "Subselects should be handled in SQLTranslator.");

  switch (expr.type) {
    case hsql::kExprOperator: {
      auto operator_type = operator_type_to_expression_type.at(expr.opType);
      node = LQPExpression::create_binary_operator(operator_type, left, right, alias);
      break;
    }
    case hsql::kExprColumnRef: {
      DebugAssert(input_node != nullptr, "Input node needs to be set");
      DebugAssert(expr.name != nullptr, "hsql::Expr::name needs to be set");

      auto table_name = expr.table != nullptr ? std::optional<std::string>(std::string(expr.table)) : std::nullopt;
      QualifiedColumnName qualified_column_name{name, table_name};
      auto column_reference = input_node->get_column(qualified_column_name);
      node = LQPExpression::create_column(column_reference, alias);
      break;
    }
    case hsql::kExprFunctionRef: {
      std::vector<std::shared_ptr<LQPExpression>> aggregate_function_arguments;
      for (auto elem : *(expr.exprList)) {
        aggregate_function_arguments.emplace_back(to_lqp_expression(*elem, input_node));
      }

      // This is currently for aggregate functions only, hence checking for arguments
      DebugAssert(aggregate_function_arguments.size(), "Aggregate functions must have arguments");

      // convert to upper-case to find mapping
      std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) { return std::toupper(c); });

      const auto aggregate_function_iter = aggregate_function_to_string.right.find(name);
      DebugAssert(aggregate_function_iter != aggregate_function_to_string.right.end(),
                  std::string("No such aggregate function '") + name + "'");

      auto aggregate_function = aggregate_function_iter->second;

      if (aggregate_function == AggregateFunction::Count && expr.distinct) {
        aggregate_function = AggregateFunction::CountDistinct;
      }

      node = LQPExpression::create_aggregate_function(aggregate_function, aggregate_function_arguments, alias);
      break;
    }
    case hsql::kExprLiteralFloat:
      node = LQPExpression::create_literal(expr.fval, alias);
      break;
    case hsql::kExprLiteralInt: {
      AllTypeVariant value;
      if (static_cast<int32_t>(expr.ival) == expr.ival) {
        value = static_cast<int32_t>(expr.ival);
      } else {
        value = expr.ival;
      }
      node = LQPExpression::create_literal(value, alias);
      break;
    }
    case hsql::kExprLiteralString:
      node = LQPExpression::create_literal(name, alias);
      break;
    case hsql::kExprLiteralNull:
      node = LQPExpression::create_literal(NULL_VALUE);
      break;
    case hsql::kExprParameter:
      node = LQPExpression::create_value_placeholder(ValuePlaceholder{static_cast<uint16_t>(expr.ival)});
      break;
    case hsql::kExprStar: {
      const auto table_name = expr.table != nullptr ? std::optional<std::string>(expr.table) : std::nullopt;
      node = LQPExpression::create_select_star(table_name);
      break;
    }
    default:
      Fail("Unsupported expression type");
      return nullptr;  // Make compiler happy
  }

  return node;
}

AllParameterVariant HSQLExprTranslator::to_all_parameter_variant(
    const hsql::Expr& expr, const std::optional<std::shared_ptr<AbstractLQPNode>>& input_node) {
  switch (expr.type) {
    case hsql::kExprLiteralInt:
      return AllTypeVariant(expr.ival);
    case hsql::kExprLiteralFloat:
      return AllTypeVariant(expr.fval);
    case hsql::kExprLiteralString:
      return AllTypeVariant(expr.name);
    case hsql::kExprLiteralNull:
      return NULL_VALUE;
    case hsql::kExprParameter:
      return ValuePlaceholder(expr.ival);
    case hsql::kExprColumnRef:
      Assert(input_node, "Cannot generate ColumnID without input_node");
      return HSQLExprTranslator::to_column_reference(expr, *input_node);
    default:
      Fail("Could not translate expression: type not supported.");
  }
}

LQPColumnReference HSQLExprTranslator::to_column_reference(const hsql::Expr& hsql_expr,
                                                           const std::shared_ptr<AbstractLQPNode>& input_node) {
  Assert(hsql_expr.isType(hsql::kExprColumnRef), "Input needs to be column ref");
  const auto qualified_column_name = to_qualified_column_name(hsql_expr);
  const auto column_reference = input_node->find_column(qualified_column_name);

  Assert(column_reference, "Couldn't resolve named column reference '" + qualified_column_name.as_string() + "'");

  return *column_reference;
}

QualifiedColumnName HSQLExprTranslator::to_qualified_column_name(const hsql::Expr& hsql_expr) {
  DebugAssert(hsql_expr.isType(hsql::kExprColumnRef), "Expression type can't be converted into column identifier");
  DebugAssert(hsql_expr.name != nullptr, "hsql::Expr::name needs to be set");

  return QualifiedColumnName{hsql_expr.name,
                             hsql_expr.table == nullptr ? std::nullopt : std::optional<std::string>(hsql_expr.table)};
}
}  // namespace opossum
