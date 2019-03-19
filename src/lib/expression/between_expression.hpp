#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  static PredicateCondition get_between_predicate_expression(bool left_inclusive, bool right_inclusive);

  static bool left_inclusive(PredicateCondition predicate_condition);
  
  static bool right_inclusive(PredicateCondition predicate_condition);

  BetweenExpression(const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound,
                    const PredicateCondition predicate_condition = PredicateCondition::BetweenInclusive);

  const std::shared_ptr<AbstractExpression>& value() const;
  const std::shared_ptr<AbstractExpression>& lower_bound() const;
  const std::shared_ptr<AbstractExpression>& upper_bound() const;
  bool left_inclusive() const;
  bool right_inclusive() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

 protected:
  ExpressionPrecedence _precedence() const override;

};

class BetweenLowerExclusiveExpression : public BetweenExpression {
 public:
  BetweenLowerExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound);

};

class BetweenUpperExclusiveExpression : public BetweenExpression {
 public:
  BetweenUpperExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound);

};

class BetweenExclusiveExpression : public BetweenExpression {
 public:
  BetweenExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound);

};

}  // namespace opossum
