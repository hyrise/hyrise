#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "SQLParser.h"

#include "all_type_variant.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"
#include "optimizer/abstract_syntax_tree/expression_node.hpp"

namespace opossum {

class SQLExpressionTranslator {
 public:
  SQLExpressionTranslator();
  virtual ~SQLExpressionTranslator();

  static std::shared_ptr<ExpressionNode> translate_expression(const hsql::Expr& expr);

 protected:
  static ExpressionType _operator_to_expression_type(hsql::OperatorType type);
};

}  // namespace opossum
