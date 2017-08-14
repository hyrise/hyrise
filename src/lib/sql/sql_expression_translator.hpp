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
  static std::shared_ptr<ExpressionNode> translate_expression(const hsql::Expr& expr, const std::shared_ptr<AbstractASTNode> &input_node);
};

}  // namespace opossum
