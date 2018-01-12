#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "all_parameter_variant.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class LQPExpression;

/**
 * Transforms hsql::Expr into various specialised opossum objects
 */
class HSQLExprTranslator {
 public:
  static std::shared_ptr<LQPExpression> to_lqp_expression(const hsql::Expr& expr,
                                                          const std::shared_ptr<AbstractLQPNode>& input_node);

  static AllParameterVariant to_all_parameter_variant(
      const hsql::Expr& expr, const std::optional<std::shared_ptr<AbstractLQPNode>>& input_node = std::nullopt);

  static LQPColumnReference to_column_reference(const hsql::Expr& hsql_expr,
                                                const std::shared_ptr<AbstractLQPNode>& input_node);

  static QualifiedColumnName to_qualified_column_name(const hsql::Expr& hsql_expr);
};

}  // namespace opossum
