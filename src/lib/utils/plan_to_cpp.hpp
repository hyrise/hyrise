#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;
class LQPColumnExpression;

using ColumnExpressionNames = std::unordered_map<std::shared_ptr<LQPColumnExpression>, std::string, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

std::string lqp_to_cpp(const std::shared_ptr<AbstractLQPNode>& lqp);
std::string expression_to_cpp(const std::shared_ptr<AbstractExpression>& expression, const ColumnExpressionNames& column_expression_names);

}  // namespace opossum