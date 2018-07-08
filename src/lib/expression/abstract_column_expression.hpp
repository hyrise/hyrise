#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * Base class for PQPColumnExpression (using a ColumnID to reference a column) and LQPColumnExpression (using an
 * LQPColumnReference to reference a column)
 */
class AbstractColumnExpression : public AbstractExpression {
 public:
  AbstractColumnExpression();

  bool requires_computation() const override;
};

}  // namespace opossum
