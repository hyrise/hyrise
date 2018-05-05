#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class DateComponent { Year, Month, Day };

class ExtractExpression : public AbstractExpression {
 public:
  ExtractExpression(const DateComponent date_component, const std::shared_ptr<AbstractExpression>& from);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  std::shared_ptr<AbstractExpression> from() const;

  DateComponent date_component;
};

}  // namespace opossum
