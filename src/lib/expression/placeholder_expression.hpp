#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * Represents a placeholder (SELECT a + ? ...) in a PreparedPlan. Will be replaced by a different expression by
 * PreparedPlan::instantiate()
 */
class PlaceholderExpression : public AbstractExpression {
 public:
  explicit PlaceholderExpression(const ParameterID parameter_id);

  bool requires_computation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const ParameterID parameter_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
