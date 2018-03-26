#pragma once

#include "abstract_column_expression.hpp"

#include "types.hpp"

namespace opossum {

class PQPColumnExpression : public AbstractColumnExpression {
 public:
  PQPColumnExpression(const ColumnID column_id);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;

  ColumnID column_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};


}  // namespace opossum
