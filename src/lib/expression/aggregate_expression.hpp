#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * Supported aggregate functions. In addition to the default SQL functions (e.g., MIN(), MAX()), Hyrise internally uses
 * the ANY() function, which expects all values in the group to be equal and returns that value. In SQL terms, this
 * would be an additional, but unnecessary GROUP BY column. This function is only used by the optimizer in case that
 * all values of the group are known to be equal (see DependentGroupByReductionRule).
 */
enum class AggregateFunction { Min, Max, Sum, Avg, Count, CountDistinct, StandardDeviationSample, Any };

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
