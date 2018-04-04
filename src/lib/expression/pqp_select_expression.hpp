#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class AbstractOperator;

class PQPSelectExpression : public AbstractExpression {
 public:
  explicit PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp);

  bool requires_calculation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;

  std::shared_ptr<AbstractOperator> pqp;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
