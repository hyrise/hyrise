#include "sql_expression_translator.hpp"

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
        node = std::make_shared<ExpressionNode>(ExpressionType::ExpressionColumnReference, table_name, name);
        break;
      case hsql::kExprFunctionRef:
      case hsql::kExprLiteralFloat:
        node = std::make_shared<ExpressionNode>(ExpressionType::ExpressionLiteral, float_value);
        break;
      case hsql::kExprLiteralInt:
        node = std::make_shared<ExpressionNode>(ExpressionType::ExpressionLiteral, int_value);
        break;
      case hsql::kExprLiteralString:
        node = std::make_shared<ExpressionNode>(ExpressionType::ExpressionLiteral, name);
        break;
      case hsql::kExprParameter:
        node = std::make_shared<ExpressionNode>(ExpressionType::ExpressionParameter, int_value);
        break;
      case hsql::kExprStar:
        node = std::make_shared<ExpressionNode>(ExpressionType::ExpressionStar);
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
  switch(type) {
    case hsql::kOpPlus: return ExpressionType::ExpressionPlus;
    case hsql::kOpMinus: return ExpressionType::ExpressionMinus;
    case hsql::kOpAsterisk: return ExpressionType::ExpressionAsterisk;
    case hsql::kOpSlash: return ExpressionType::ExpressionSlash;
    case hsql::kOpPercentage: return ExpressionType::ExpressionPercentage;
    case hsql::kOpCaret: return ExpressionType::ExpressionCaret;
    case hsql::kOpBetween: return ExpressionType::ExpressionBetween;
    case hsql::kOpEquals: return ExpressionType::ExpressionEquals;
    case hsql::kOpNotEquals: return ExpressionType::ExpressionNotEquals;
    case hsql::kOpLess: return ExpressionType::ExpressionLess;
    case hsql::kOpLessEq: return ExpressionType::ExpressionLessEq;
    case hsql::kOpGreater: return ExpressionType::ExpressionGreater;
    case hsql::kOpGreaterEq: return ExpressionType::ExpressionGreaterEq;
    case hsql::kOpLike: return ExpressionType::ExpressionLike;
    case hsql::kOpNotLike: return ExpressionType::ExpressionNotLike;
    case hsql::kOpCase: return ExpressionType::ExpressionCase;
    case hsql::kOpExists:
    case hsql::kOpIn:
    case hsql::kOpIsNull:
    case hsql::kOpOr:
    default: throw std::runtime_error("Not support OperatorType");
  }
}

}  // namespace opossum
