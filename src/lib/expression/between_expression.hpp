#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class BaseBetweenExpression : public AbstractPredicateExpression {
 public:
  static PredicateCondition get_between_predicate_expression(bool left_inclusive, bool right_inclusive);

  static bool is_between_predicate_expression(PredicateCondition predicate_condition);

  static bool left_inclusive(PredicateCondition predicate_condition);

  static bool right_inclusive(PredicateCondition predicate_condition);

  BaseBetweenExpression(const std::shared_ptr<AbstractExpression>& value,
                        const std::shared_ptr<AbstractExpression>& lower_bound,
                        const std::shared_ptr<AbstractExpression>& upper_bound,
                        const PredicateCondition predicate_condition);

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

class BetweenInclusiveExpression : public BaseBetweenExpression {
 public:
  BetweenInclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound);
};

class BetweenLowerExclusiveExpression : public BaseBetweenExpression {
 public:
  BetweenLowerExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                                  const std::shared_ptr<AbstractExpression>& lower_bound,
                                  const std::shared_ptr<AbstractExpression>& upper_bound);
};

class BetweenUpperExclusiveExpression : public BaseBetweenExpression {
 public:
  BetweenUpperExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                                  const std::shared_ptr<AbstractExpression>& lower_bound,
                                  const std::shared_ptr<AbstractExpression>& upper_bound);
};

class BetweenExclusiveExpression : public BaseBetweenExpression {
 public:
  BetweenExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                             const std::shared_ptr<AbstractExpression>& lower_bound,
                             const std::shared_ptr<AbstractExpression>& upper_bound);
};

}  // namespace opossum
