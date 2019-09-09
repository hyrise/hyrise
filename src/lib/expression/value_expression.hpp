#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace opossum {

/**
 * Wraps an AllTypeVariant
 */
class ValueExpression : public AbstractExpression {
 public:
  explicit ValueExpression(const AllTypeVariant& value);

  bool requires_computation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  const AllTypeVariant value;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
};

}  // namespace opossum
