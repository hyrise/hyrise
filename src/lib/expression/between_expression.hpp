#pragma once

#include "../operators/property.hpp"
#include "../visualization/serializer/json_serializer.hpp"
#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace opossum {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  BetweenExpression(const PredicateCondition init_predicate_condition, const std::shared_ptr<AbstractExpression>& value,
                    const std::shared_ptr<AbstractExpression>& lower_bound,
                    const std::shared_ptr<AbstractExpression>& upper_bound);

  const std::shared_ptr<AbstractExpression>& value() const;
  const std::shared_ptr<AbstractExpression>& lower_bound() const;
  const std::shared_ptr<AbstractExpression>& upper_bound() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence _precedence() const override;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members. Done?
  inline constexpr static auto properties = std::make_tuple(
      // from AbstractPredicateExpression
      property(&BetweenExpression::predicate_condition, "predicate_condition"),
      // From AbstractExpression
      property(&BetweenExpression::arguments, "arguments"), property(&BetweenExpression::type, "type"));
};

}  // namespace opossum
