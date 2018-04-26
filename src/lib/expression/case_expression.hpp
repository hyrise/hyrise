#pragma once

#include <optional>

#include "boost/variant.hpp"

#include "abstract_expression.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

class CaseExpression : public AbstractExpression {
 public:
  CaseExpression(const std::shared_ptr<AbstractExpression>& when,
                          const std::shared_ptr<AbstractExpression>& then,
                          const std::shared_ptr<AbstractExpression>& else_);

  const std::shared_ptr<AbstractExpression>& when() const;
  const std::shared_ptr<AbstractExpression>& then() const;
  const std::shared_ptr<AbstractExpression>& else_() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  ExpressionDataTypeVariant data_type() const override;
};

}  // namespace opossum
