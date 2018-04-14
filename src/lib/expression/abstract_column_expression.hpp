#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class AbstractColumnExpression : public AbstractExpression {
 public:
  AbstractColumnExpression(): AbstractExpression(ExpressionType::Column, {}) {}
};

}  // namespace opossum
