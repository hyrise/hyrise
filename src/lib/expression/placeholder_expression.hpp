#pragma once

#include "abstract_parameter_expression.hpp"

namespace opossum {

/**
 * Represents a placeholder (SELECT a + ? ...).
 */
class PlaceholderExpression : public AbstractParameterExpression {
 public:
  explicit PlaceholderExpression(const ParameterID parameter_id);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
