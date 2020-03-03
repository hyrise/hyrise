#pragma once

#include "placeholder_expression.hpp"

namespace opossum {

/**
 * Represents a placeholder (SELECT a + ? ...) in a PreparedPlan. Will be replaced by a different expression by
 * PreparedPlan::instantiate()
 */
class TypedPlaceholderExpression : public PlaceholderExpression {
 public:
  explicit TypedPlaceholderExpression(const ParameterID parameter_id, DataType data_type);
  DataType data_type() const override;

 protected:
  DataType _data_type;
};

}  // namespace opossum
