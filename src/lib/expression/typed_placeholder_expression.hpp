#pragma once

#include "placeholder_expression.hpp"

namespace opossum {

/**
 * Represents a placeholder (SELECT a + ? ...) in a PreparedPlan. Will be replaced by a different expression by
 * PreparedPlan::instantiate()
 */
class TypedPlaceholderExpression : public PlaceholderExpression {
 public:
  explicit TypedPlaceholderExpression(const ParameterID init_parameter_id, const DataType init_data_type);
  DataType data_type() const override;
  std::string description(const DescriptionMode mode) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;

 protected:
  const DataType _data_type;

  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
};

}  // namespace opossum
