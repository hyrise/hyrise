#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace hyrise {

/**
 * Wraps an AllTypeVariant
 */
class ValueExpression : public AbstractExpression {
 public:
  explicit ValueExpression(const AllTypeVariant& init_value);

  bool requires_computation() const override;
  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  const AllTypeVariant value;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
};

}  // namespace hyrise
