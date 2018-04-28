#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class ValuePlaceholderExpression : public AbstractExpression {
 public:
  explicit ValuePlaceholderExpression(const ValuePlaceholder& value_placeholder);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  ValuePlaceholder value_placeholder;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
