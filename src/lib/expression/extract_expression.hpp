#pragma once

#include <ostream>

#include "abstract_expression.hpp"

namespace opossum {

enum class DatetimeComponent { Year, Month, Day, Hour, Minute, Second };

std::ostream& operator<<(std::ostream& stream, const DatetimeComponent datetime_component);

/**
 * SQL's EXTRACT()
 * NOT a FunctionExpression since we currently have no way for taking as an enum such as DatetimeComponent as a function
 * argument
 */
class ExtractExpression : public AbstractExpression {
 public:
  ExtractExpression(const DatetimeComponent datetime_component, const std::shared_ptr<AbstractExpression>& from);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  std::shared_ptr<AbstractExpression> from() const;

  const DatetimeComponent datetime_component;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
};

}  // namespace opossum
