#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class NullExpression : public AbstractExpression {
 public:
  NullExpression();

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;
};

}  // namespace opossum
