#pragma once

#include <ostream>

#include "abstract_expression.hpp"

namespace opossum {

enum class DatetimeComponent { Year, Month, Day, Hour, Minute, Second };

std::ostream& operator<<(std::ostream& stream, const DatetimeComponent datetime_component);

/**
 * SQL's EXTRACT()
 */
class ExtractExpression : public AbstractExpression {
 public:
  ExtractExpression(const DatetimeComponent datetime_component, const std::shared_ptr<AbstractExpression>& from);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  std::shared_ptr<AbstractExpression> from() const;

  DatetimeComponent datetime_component;
};

}  // namespace opossum
