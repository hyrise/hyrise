#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class AggregateFunction {
  Min, Max, Sum, Avg, Count, CountDistinct
};

class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(const AggregateFunction aggregate_function,
                      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  AggregateFunction aggregate_function;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

} // namespace opossum
