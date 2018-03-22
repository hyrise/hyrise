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
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/

  std::shared_ptr<AbstractExpression> value;
  std::shared_ptr<AbstractExpression> set;
};

}  // namespace opossum
