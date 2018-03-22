#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class ValuePlaceholderExpression : public AbstractExpression {
 public:
  explicit ValuePlaceholderExpression(const ValuePlaceholder value_placeholder);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/

  ValuePlaceholder value_placeholder;
};

}  // namespace opossum
