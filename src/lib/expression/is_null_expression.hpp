#pragma once

#include "abstract_predicate_expression.hpp" // NEEDEDINCLUDE

namespace opossum {

class IsNullExpression : public AbstractPredicateExpression {
 public:
  IsNullExpression(const PredicateCondition predicate_condition, const std::shared_ptr<AbstractExpression>& operand);

  const std::shared_ptr<AbstractExpression>& operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

 protected:
  ExpressionPrecedence _precedence() const override;
};

}  // namespace opossum
