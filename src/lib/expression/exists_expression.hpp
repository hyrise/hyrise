#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class SelectExpression;

class ExistsExpression : public AbstractExpression {
  explicit ExistsExpression(const std::shared_ptr<SelectExpression>& select);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/

  std::shared_ptr<SelectExpression> select;
};

}  // namespace opossum
