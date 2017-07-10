#include "sql_expression_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/abstract_syntax_tree/expression_node.hpp"

#include "SQLParser.h"

using hsql::Expr;

namespace opossum {

SQLExpressionTranslator::SQLExpressionTranslator() {}

SQLExpressionTranslator::~SQLExpressionTranslator() {}

std::shared_ptr<ExpressionNode> SQLExpressionTranslator::translate_expression(const hsql::Expr& expr) {
  auto table_name = (expr.table) ? std::string(expr.table) : std::string("");
  auto name = (expr.name) ? std::string(expr.name) : std::string("");
  auto float_value = (expr.fval) ? expr.fval : 0;
  auto int_value = (expr.ival) ? expr.ival : 0;

  std::shared_ptr<ExpressionNode> node;
  if (expr.type == hsql::kExprOperator) {
    auto operatorType = _operator_to_expression_type(expr.opType);
    node = std::make_shared<ExpressionNode>(operatorType);
  } else {
    switch (expr.type) {
      case hsql::kExprColumnRef:
        node = std::make_shared<ExpressionNode>(ExpressionType::ColumnReference, table_name, name);
        break;
      case hsql::kExprFunctionRef: {
        // TODO(mp): Parse Function name to Aggregate Function

        auto expression_list = std::make_shared<std::vector<std::shared_ptr<ExpressionNode>>>();
        for (auto elem : *(expr.exprList)) {
          auto node = translate_expression(*elem);
          expression_list->emplace_back(node);
        }

        node = std::make_shared<ExpressionNode>(ExpressionType::FunctionReference, name, expression_list);
        break;
      }
      case hsql::kExprLiteralFloat:
        node = std::make_shared<ExpressionNode>(ExpressionType::Literal, float_value);
        break;
      case hsql::kExprLiteralInt:
        node = std::make_shared<ExpressionNode>(ExpressionType::Literal, int_value);
        break;
      case hsql::kExprLiteralString:
        node = std::make_shared<ExpressionNode>(ExpressionType::Literal, name);
        break;
      case hsql::kExprParameter:
        node = std::make_shared<ExpressionNode>(ExpressionType::Parameter, int_value);
        break;
      case hsql::kExprStar:
        node = std::make_shared<ExpressionNode>(ExpressionType::Star);
        break;
      case hsql::kExprSelect:
      case hsql::kExprHint:
      default:
        throw std::runtime_error("Unsupported expression type");
    }
  }

  if (expr.expr) {
    auto left = translate_expression(*expr.expr);
    node->set_left(left);
  }

  if (expr.expr2) {
    auto right = translate_expression(*expr.expr2);
    node->set_right(right);
  }

  return node;
}

ExpressionType SQLExpressionTranslator::_operator_to_expression_type(hsql::OperatorType type) {
  switch (type) {
    case hsql::kOpPlus:
      return ExpressionType::Plus;
    case hsql::kOpMinus:
      return ExpressionType::Minus;
    case hsql::kOpAsterisk:
      return ExpressionType::Asterisk;
    case hsql::kOpSlash:
      return ExpressionType::Slash;
    case hsql::kOpPercentage:
      return ExpressionType::Percentage;
    case hsql::kOpCaret:
      return ExpressionType::Caret;
    case hsql::kOpBetween:
      return ExpressionType::Between;
    case hsql::kOpEquals:
      return ExpressionType::Equals;
    case hsql::kOpNotEquals:
      return ExpressionType::NotEquals;
    case hsql::kOpLess:
      return ExpressionType::Less;
    case hsql::kOpLessEq:
      return ExpressionType::LessEquals;
    case hsql::kOpGreater:
      return ExpressionType::Greater;
    case hsql::kOpGreaterEq:
      return ExpressionType::GreaterEquals;
    case hsql::kOpLike:
      return ExpressionType::Like;
    case hsql::kOpNotLike:
      return ExpressionType::NotLike;
    case hsql::kOpCase:
      return ExpressionType::Case;
    case hsql::kOpExists:
    case hsql::kOpIn:
    case hsql::kOpIsNull:
    case hsql::kOpOr:
    default:
      throw std::runtime_error("Not support OperatorType");
  }
}

}  // namespace opossum
