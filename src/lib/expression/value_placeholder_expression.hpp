#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class ValuePlaceholderExpression : public AbstractExpression {
 public:
  explicit ValuePlaceholderExpression(const ValuePlaceholder& value_placeholder);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/

  ValuePlaceholder value_placeholder;
};

}  // namespace opossum
