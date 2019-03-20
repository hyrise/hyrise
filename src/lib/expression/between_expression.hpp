#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

bool is_between_predicate_expression(PredicateCondition predicate_condition);

bool is_between_predicate_expression_left_inclusive(PredicateCondition predicate_condition);

bool is_between_predicate_expression_right_inclusive(PredicateCondition predicate_condition);

class BetweenExpression : public AbstractPredicateExpression {
 public:
  // the SQL standard only know inclusive betweens, therefore predicate_condition defaults to inclusive between
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

}  // namespace opossum
