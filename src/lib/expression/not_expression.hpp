#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class NotExpression : public AbstractExpression {
 public:
  NotExpression(const std::shared_ptr<AbstractExpression>& operand);

  const std::shared_ptr<AbstractExpression>& operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
