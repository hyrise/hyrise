#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class ArithmeticExpression : public AbstractExpression {


  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/
};

}  // namespace opossum
