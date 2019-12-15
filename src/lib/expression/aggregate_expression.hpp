#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class AggregateFunction { Min, Max, Sum, Avg, Count, CountDistinct, StandardDeviationSample };

class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(const AggregateFunction aggregate_function, const std::shared_ptr<AbstractExpression>& argument);

  std::shared_ptr<AbstractExpression> argument() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  const AggregateFunction aggregate_function;

  static bool is_count_star(const AbstractExpression& expression);

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
};

}  // namespace opossum
