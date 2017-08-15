#include "sql_expression_translator.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <utils/assert.hpp>

#include "constant_mappings.hpp"
#include "optimizer/expression/expression_node.hpp"

#include "SQLParser.h"

namespace opossum {

std::shared_ptr<ExpressionNode> SQLExpressionTranslator::translate_expression(const hsql::Expr& expr, const std::shared_ptr<AbstractASTNode> &input_node) {
  auto table_name = expr.table ? std::string(expr.table) : "";
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
      ColumnIdentifier column_identifier {table_name, name};
      auto column_id = input_node->find_column_id_for_column_identifier(column_identifier);
      if (!column_id) { Fail("Did not find column " + name); }
      node = ExpressionNode::create_column_reference(*column_id, alias);
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

}  // namespace opossum
