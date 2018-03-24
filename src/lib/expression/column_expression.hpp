#pragma once

#include "abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

template<typename ColumnReference>
class ColumnExpression : public AbstractExpression {
  explicit ColumnExpression(const ColumnReference& column):
    AbstractExpression(ExpressionType::Column, {}), column(column) {}

  std::shared_ptr<AbstractExpression> deep_copy() const override {
    return std::make_shared<ColumnExpression>(column);
  }

  ColumnReference column;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override {
    return column = static_cast<const ColumnExpression&>(expression).column;
  }
};

using LQPColumnExpression = ColumnExpression<LQPColumnReference>;
using PQPColumnExpression = ColumnExpression<ColumnID>;

}  // namespace opossum
