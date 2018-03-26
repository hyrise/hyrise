#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class AbstractColumnExpression : public AbstractExpression {
  AbstractColumnExpression(): AbstractExpression(ExpressionType::Column, {}) {}
};

}  // namespace opossum
