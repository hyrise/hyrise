#pragma once

#include "../operators/property.hpp"
#include "../visualization/serializer/json_serializer.hpp"
#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class IsNullExpression : public AbstractPredicateExpression {
 public:
  IsNullExpression(const PredicateCondition init_predicate_condition,
                   const std::shared_ptr<AbstractExpression>& operand);

  const std::shared_ptr<AbstractExpression>& operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence _precedence() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members. Done?
  inline constexpr static auto properties = std::make_tuple(
      // from AbstractPredicateExpression
      property(&IsNullExpression::predicate_condition, "predicate_condition"),
      // From AbstractExpression
      property(&IsNullExpression::arguments, "arguments"), property(&IsNullExpression::type, "type"));
};

}  // namespace opossum
