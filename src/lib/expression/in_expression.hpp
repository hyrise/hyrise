#pragma once

#include "../operators/property.hpp"
#include "../visualization/serializer/json_serializer.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

/**
 * SQL's IN
 */
class InExpression : public AbstractPredicateExpression {
 public:
  InExpression(const PredicateCondition init_predicate_condition, const std::shared_ptr<AbstractExpression>& value,
               const std::shared_ptr<AbstractExpression>& set);

  /**
   * Utility for better readability
   * @return predicate_condition == PredicateCondition::NotIn
   */
  bool is_negated() const;

  const std::shared_ptr<AbstractExpression>& value() const;
  const std::shared_ptr<AbstractExpression>& set() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members. Done?
  inline constexpr static auto properties = std::make_tuple(
      // from AbstractPredicateExpression
      property(&InExpression::predicate_condition, "predicate_condition"),
      // From AbstractExpression
      property(&InExpression::arguments, "arguments"), property(&InExpression::type, "type"));
};

}  // namespace opossum
