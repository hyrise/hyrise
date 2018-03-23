#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class InExpression : public AbstractExpression {
 public:
  InExpression(const std::shared_ptr<AbstractExpression>& value, const std::shared_ptr<AbstractExpression>& set);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/

  std::shared_ptr<AbstractExpression> value;
  std::shared_ptr<AbstractExpression> set;
};

}  // namespace opossum
