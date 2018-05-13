#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class AbstractSelectExpression: public AbstractExpression {
 public:
  AbstractSelectExpression();
  explicit AbstractSelectExpression(const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions);

  bool requires_calculation() const override;
};

}  // namespace opossum