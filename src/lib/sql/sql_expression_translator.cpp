#include "sql_expression_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "constant_mappings.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

std::shared_ptr<ExpressionNode> SQLExpressionTranslator::translate_expression(
    const hsql::Expr &expr, const std::shared_ptr<AbstractASTNode> &input_node) {
  auto name = expr.name ? std::string(expr.name) : "";
  auto float_value = expr.fval ? expr.fval : 0;
  auto int_value = expr.ival ? expr.ival : 0;
  auto alias = expr.alias ? optional<std::string>(expr.alias) : nullopt;

  std::shared_ptr<ExpressionNode> node;
  switch (expr.type) {
    case hsql::kExprOperator: {
      auto operator_type = operator_type_to_expression_type.at(expr.opType);
      node = ExpressionNode::create_expression(operator_type);
      break;
    }
    case hsql::kExprColumnRef: {
      DebugAssert(input_node != nullptr, "Input node needs to be set");
      DebugAssert(expr.name != nullptr, "hsql::Expr::name needs to be set");

      auto table_name = expr.table != nullptr ? optional<std::string>(std::string(expr.table)) : nullopt;
      ColumnIdentifier column_identifier{name, table_name};
      auto column_id = input_node->get_column_id_for_column_identifier(column_identifier);
      node = ExpressionNode::create_column_identifier(column_id, alias);
      break;
    }
    case hsql::kExprFunctionRef: {
      // TODO(mp): Parse Function name to Aggregate Function
      // auto aggregate_function = string_to_aggregate_function.at(name);

      std::vector<std::shared_ptr<ExpressionNode>> expression_list;
      for (auto elem : *(expr.exprList)) {
        expression_list.emplace_back(translate_expression(*elem, input_node));
      }

      node = ExpressionNode::create_function_reference(name, expression_list, alias);
      break;
    }
    case hsql::kExprLiteralFloat:
      node = ExpressionNode::create_literal(float_value);
      break;
    case hsql::kExprLiteralInt:
      node = ExpressionNode::create_literal(int_value);
      break;
    case hsql::kExprLiteralString:
      node = ExpressionNode::create_literal(name);
      break;
    case hsql::kExprParameter:
      node = ExpressionNode::create_parameter(int_value);
      break;
    case hsql::kExprStar:
      node = ExpressionNode::create_expression(ExpressionType::Star);
      break;
    case hsql::kExprSelect:
      /**
       * Current problem with Subselect:
       *
       * For now we split Expressions and ASTNodes into two separate trees.
       * The only connection is the PredicateNode that contains an ExpressionNode.
       *
       * When we translate Subselects, the naive approach would be to add another member to the ExpressionNode,
       * which is a Pointer to the root node of the AST of the Subselect, so usually a ProjectionNode.
       *
       * Right now, I cannot estimate the consequences of such a circular reference for the optimizer rules.
       */
      // TODO(mp): translate as soon as SQLToASTTranslator is merged
      throw std::runtime_error("Selects are not supported yet.");
    default:
      throw std::runtime_error("Unsupported expression type");
  }

  if (expr.expr) {
    auto left = translate_expression(*expr.expr, input_node);
    node->set_left_child(left);
  }

  if (expr.expr2) {
    auto right = translate_expression(*expr.expr2, input_node);
    node->set_right_child(right);
  }

  return node;
}

ColumnIdentifier SQLExpressionTranslator::get_column_identifier_for_column_ref(const hsql::Expr &hsql_expr) {
  DebugAssert(hsql_expr.isType(hsql::kExprColumnRef), "Expression type can't be converted into column identifier");
  DebugAssert(hsql_expr.name != nullptr, "hsql::Expr::name needs to be set");

  return ColumnIdentifier{hsql_expr.name,
                          hsql_expr.table == nullptr ? nullopt : optional<std::string>(hsql_expr.table)};
}

ColumnID SQLExpressionTranslator::get_column_id_for_expression(const hsql::Expr &hsql_expr,
                                                               const std::shared_ptr<AbstractASTNode> &input_node) {
  if (hsql_expr.isType(hsql::kExprColumnRef)) {
    return input_node->get_column_id_for_column_identifier(get_column_identifier_for_column_ref(hsql_expr));
  }

  auto expr = translate_expression(hsql_expr, input_node);
  return input_node->get_column_id_for_expression(expr);
}

}  // namespace opossum
