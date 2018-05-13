#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class ExternalExpression : public AbstractExpression {
 public:
  explicit ExternalExpression(const ValuePlaceholder& value_placeholder,
  const DataType data_type,
  const bool nullable,
  const std::string& column_name);

  const std::shared_ptr<AbstractExpression>& referenced_expression() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  ValuePlaceholder value_placeholder;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const DataType _data_type;
  const bool _nullable;
  const std::string _column_name;
};

}  // namespace opossum
