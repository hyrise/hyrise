#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class PredicateExpression : public AbstractExpression {
 public:
  PredicateExpression(const PredicateCondition predicate_condition,
                      const std::shared_ptr<AbstractExpression>& left_operand,
                      const std::shared_ptr<AbstractExpression>& right_operand);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/

  PredicateCondition predicate_condition;
  std::shared_ptr<AbstractExpression> left_operand;
  std::shared_ptr<AbstractExpression> right_operand;
};

}  // namespace opossum
