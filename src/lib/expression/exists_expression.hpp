#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class LQPSelectExpression;

class ExistsExpression : public AbstractExpression {
  explicit ExistsExpression(const std::shared_ptr<LQPSelectExpression>& select);

  const std::shared_ptr<LQPSelectExpression>& select() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;
};

}  // namespace opossum
