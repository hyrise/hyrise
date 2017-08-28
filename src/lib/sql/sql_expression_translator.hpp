#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"

#include "all_type_variant.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/expression/expression_node.hpp"

namespace opossum {

class SQLExpressionTranslator {
 public:
  static std::shared_ptr<ExpressionNode> translate_expression(const hsql::Expr &expr,
                                                              const std::shared_ptr<AbstractASTNode> &input_node);

  // Helper. Asserts that hsql_expr is a ColumnRef, constructs a ColumnIdentifier from it
  static ColumnIdentifierName get_column_identifier_name_for_column_ref(const hsql::Expr &hsql_expr);

  // Helper. Converts hsql_expr into ExpressionNode and looks for it in input_node's output
  static ColumnID get_column_id_for_expression(const hsql::Expr &hsql_expr,
                                               const std::shared_ptr<AbstractASTNode> &input_node);
};

}  // namespace opossum
