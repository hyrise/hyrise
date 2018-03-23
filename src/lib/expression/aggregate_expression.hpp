#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class AggregateType {
  Min, Max, Sum, Avg, Count, CountDistinct
};

class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(const AggregateType aggregate_type,
                      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/

  AggregateType aggregate_type;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;
};

} // namespace opossum
