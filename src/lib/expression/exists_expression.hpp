#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class SelectExpression;

class ExistsExpression : public AbstractExpression {
  explicit ExistsExpression(const std::shared_ptr<SelectExpression>& select);

  const std::shared_ptr<SelectExpression>& select() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
};

}  // namespace opossum
