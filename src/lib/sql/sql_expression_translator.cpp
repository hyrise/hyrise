#include "sql_expression_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/expression/expression.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

std::shared_ptr<Expression> SQLExpressionTranslator::translate_expression(
    const hsql::Expr &expr, const std::shared_ptr<AbstractASTNode> &input_node) {
  auto name = expr.name ? std::string(expr.name) : "";
  auto float_value = expr.fval ? expr.fval : 0;
  auto int_value = expr.ival ? expr.ival : 0;
  auto alias = expr.alias ? optional<std::string>(expr.alias) : nullopt;

  std::shared_ptr<Expression> node;
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;

  if (expr.expr) {
    left = translate_expression(*expr.expr, input_node);
  }

  if (expr.expr2) {
    right = translate_expression(*expr.expr2, input_node);
  }

  switch (expr.type) {
    case hsql::kExprOperator: {
      auto operator_type = operator_type_to_expression_type.at(expr.opType);
      node = Expression::create_binary_operator(operator_type, left, right);
      break;
    }
    case hsql::kExprColumnRef: {
      DebugAssert(input_node != nullptr, "Input node needs to be set");
      DebugAssert(expr.name != nullptr, "hsql::Expr::name needs to be set");

      auto table_name = expr.table != nullptr ? optional<std::string>(std::string(expr.table)) : nullopt;
      ColumnIdentifierName column_identifier_name{name, table_name};
      auto column_id = input_node->get_column_id_for_column_identifier_name(column_identifier_name);
      node = Expression::create_column_identifier(column_id, alias);
      break;
    }
    case hsql::kExprFunctionRef: {
      // TODO(mp): Parse Function name to Aggregate Function
      // auto aggregate_function = string_to_aggregate_function.at(name);

      std::vector<std::shared_ptr<Expression>> expression_list;
      for (auto elem : *(expr.exprList)) {
        expression_list.emplace_back(translate_expression(*elem, input_node));
      }

      node = Expression::create_function(name, expression_list, alias);
      break;
    }
    case hsql::kExprLiteralFloat:
      node = Expression::create_literal(float_value);
      break;
    case hsql::kExprLiteralInt:
      node = Expression::create_literal(int_value);
      break;
    case hsql::kExprLiteralString:
      node = Expression::create_literal(name);
      break;
    case hsql::kExprParameter:
      node = Expression::create_placeholder(int_value);
      break;
    case hsql::kExprStar: {
      const auto table_name = expr.table != nullptr ? std::string(expr.table) : "";
      node = Expression::create_select_star(table_name);
      break;
    }
    case hsql::kExprSelect:
      /**
       * Current problem with Subselect:
       *
       * For now we split Expressions and ASTNodes into two separate trees.
       * The only connection is the PredicateNode that contains an Expression.
       *
       * When we translate Subselects, the naive approach would be to add another member to the Expression,
       * which is a Pointer to the root node of the AST of the Subselect, so usually a ProjectionNode.
       *
       * Right now, I cannot estimate the consequences of such a circular reference for the optimizer rules.
       */
      // TODO(mp): translate as soon as SQLToASTTranslator is merged
      throw std::runtime_error("Selects are not supported yet.");
    default:
      throw std::runtime_error("Unsupported expression type");
  }

  return node;
}

ColumnIdentifierName SQLExpressionTranslator::get_column_identifier_name_for_column_ref(const hsql::Expr &hsql_expr) {
  DebugAssert(hsql_expr.isType(hsql::kExprColumnRef), "Expression type can't be converted into column identifier");
  DebugAssert(hsql_expr.name != nullptr, "hsql::Expr::name needs to be set");

  return ColumnIdentifierName{hsql_expr.name,
                              hsql_expr.table == nullptr ? nullopt : optional<std::string>(hsql_expr.table)};
}

ColumnID SQLExpressionTranslator::get_column_id_for_expression(const hsql::Expr &hsql_expr,
                                                               const std::shared_ptr<AbstractASTNode> &input_node) {
  Assert(hsql_expr.isType(hsql::kExprColumnRef), "Input needs to be column ref");

  return input_node->get_column_id_for_column_identifier_name(get_column_identifier_name_for_column_ref(hsql_expr));
}

}  // namespace opossum
