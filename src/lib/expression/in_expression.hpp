#pragma once

#include "abstract_predicate_expression.hpp"

namespace opossum {

/**
 * SQL's IN
 */
class InExpression : public AbstractPredicateExpression {
 public:
  InExpression(const std::shared_ptr<AbstractExpression>& value, const std::shared_ptr<AbstractExpression>& set);

  const std::shared_ptr<AbstractExpression>& value() const;
  const std::shared_ptr<AbstractExpression>& set() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
