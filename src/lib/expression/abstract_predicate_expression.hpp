#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

/**
 * PredicateExpressions are those using a PredicateCondition.
 */
class AbstractPredicateExpression : public AbstractExpression {
 public:
  AbstractPredicateExpression(const PredicateCondition predicate_condition,
                              const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  DataType data_type() const override;

  const PredicateCondition predicate_condition;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
