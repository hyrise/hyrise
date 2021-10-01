#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class BinaryPredicateExpression : public AbstractPredicateExpression {
 public:
  BinaryPredicateExpression(const PredicateCondition init_predicate_condition,
                            const std::shared_ptr<AbstractExpression>& left_operand,
                            const std::shared_ptr<AbstractExpression>& right_operand,
                            const size_t init_skip_chars_for_like = 0);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  std::string description(const DescriptionMode mode) const override;

  size_t skip_chars_for_like;

 protected:
  ExpressionPrecedence _precedence() const override;
};

}  // namespace opossum
