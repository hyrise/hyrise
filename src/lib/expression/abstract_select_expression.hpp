#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class AbstractSelectExpression: public AbstractExpression {
 public:
  AbstractSelectExpression();

  bool requires_calculation() const override;
};

}  // namespace opossum