#pragma once

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

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
};

}  // namespace opossum
