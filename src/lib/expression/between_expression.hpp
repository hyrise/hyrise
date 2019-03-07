#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  BetweenExpression(const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound, bool left_inclusive = true,
                    bool right_inclusive = true);

  const std::shared_ptr<AbstractExpression>& value() const;
  const std::shared_ptr<AbstractExpression>& lower_bound() const;
  const std::shared_ptr<AbstractExpression>& upper_bound() const;
  bool left_inclusive() const;
  bool right_inclusive() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

 protected:
  ExpressionPrecedence _precedence() const override;

  const bool _left_inclusive;
  const bool _right_inclusive;
};

}  // namespace opossum
