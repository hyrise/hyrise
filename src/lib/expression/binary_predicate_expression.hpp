#pragma once

#include "../operators/property.hpp"
#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class BinaryPredicateExpression : public AbstractPredicateExpression {
 public:
  BinaryPredicateExpression(const PredicateCondition init_predicate_condition,
                            const std::shared_ptr<AbstractExpression>& left_operand,
                            const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence _precedence() const override;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members. Done?
  inline constexpr static auto properties = std::make_tuple(
      // from AbstractPredicateExpression
      property(&BinaryPredicateExpression::predicate_condition, "predicate_condition"),
      // From AbstractExpression
      property(&BinaryPredicateExpression::arguments, "arguments"), property(&BinaryPredicateExpression::type, "type"));
};

}  // namespace opossum
