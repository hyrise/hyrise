#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class AggregateType {
  Min, Max, Sum, Avg, Count, CountDistinct
};

class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(const AggregateType aggregate_type,
                      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  ExpressionDataTypeVariant data_type() const override;

  AggregateType aggregate_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

} // namespace opossum
