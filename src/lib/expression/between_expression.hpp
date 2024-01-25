#pragma once

#include <string>
#include <unordered_map>

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace hyrise {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  BetweenExpression(const PredicateCondition init_predicate_condition,
                    const std::shared_ptr<AbstractExpression>& operand,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound);

  const std::shared_ptr<AbstractExpression>& operand() const;
  const std::shared_ptr<AbstractExpression>& lower_bound() const;
  const std::shared_ptr<AbstractExpression>& upper_bound() const;

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence _precedence() const override;
};

}  // namespace hyrise
