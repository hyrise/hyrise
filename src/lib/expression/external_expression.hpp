#pragma once

#include "boost/variant.hpp"

#include "abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

class ExternalExpression : public AbstractExpression {
 public:
  ExternalExpression(const std::shared_ptr<AbstractExpression>& referenced_expression);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

  std::shared_ptr<AbstractExpression> referenced_expression() const;
};

}  // namespace opossum
