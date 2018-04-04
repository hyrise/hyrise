#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  BetweenExpression(const std::shared_ptr<AbstractExpression>& value,
                            const std::shared_ptr<AbstractExpression>& lower_bound,
                            const std::shared_ptr<AbstractExpression>& upper_bound);

  const std::shared_ptr<AbstractExpression>& value() const;
  const std::shared_ptr<AbstractExpression>& lower_bound() const;
  const std::shared_ptr<AbstractExpression>& upper_bound() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;
};

}  // namespace opossum
