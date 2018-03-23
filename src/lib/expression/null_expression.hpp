#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class NullExpression : public AbstractExpression {
 public:
  NullExpression();

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/
};

}  // namespace opossum
