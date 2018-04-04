#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class ArrayExpression : public AbstractExpression {
 public:
  ArrayExpression(const std::vector<std::shared_ptr<AbstractExpression>>& values);

  const std::vector<std::shared_ptr<AbstractExpression>>& values() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;
};

}  // namespace opossum
