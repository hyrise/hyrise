#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class FunctionType {
  // Aggregates
  Min, Max, Sum, Avg, Count, CountDistinct

  // Others
  Substring, Extract
};

class FunctionExpression : public AbstractExpression {
 public:
  FunctionExpression(const FunctionType function_type,
                      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/

  FunctionType function_type;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;
};

} // namespace opossum
