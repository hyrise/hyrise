#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class SQLExpressionTranslator {
 public:
  static std::shared_ptr<Expression> translate_expression(const hsql::Expr& expr,
                                                          const std::shared_ptr<AbstractLQPNode>& input_node);

  // Helper. Asserts that hsql_expr is a ColumnRef, constructs a NamedColumnReference from it
  static NamedColumnReference get_named_column_reference_for_column_reference(const hsql::Expr& hsql_expr);

  // Helper. Converts hsql_expr into Expression and looks for it in input_node's output
  static ColumnID get_column_id_for_expression(const hsql::Expr& hsql_expr,
                                               const std::shared_ptr<AbstractLQPNode>& input_node);
};

}  // namespace opossum
