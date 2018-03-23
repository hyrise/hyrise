#pragma once

#include "abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

template<typename ColumnReference>
class ColumnExpression : public AbstractExpression {
  explicit ColumnExpression(const ColumnReference& column):
    AbstractExpression(ExpressionType::Column), column(column) {}

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override {
    const auto* column_expression = dynamic_cast<const ColumnExpression<ColumnReference>*>(&expression);
    if (!column_expression) return false;
    return column_expression->column == column;
  }

  std::shared_ptr<AbstractExpression> deep_copy() const override {
    return std::make_shared<ColumnExpression>(column);
  }

  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override {
    if constexpr (std::is_same_v<ColumnReference, LQPColumnReference>) {
      const auto referenced_expression = column.original_node()->output_column_expressions()[column.original_column_id()];
      if (referenced_expression->type() == ExpressionType::Column) {
        const auto referenced_column_expression = std::static_pointer_cast<ColumnExpression<LQPColumnReference>>(referenced_expression);
        if (referenced_column_expression.column != column) {
          return referenced_column_expression->deep_copy()->deep_resolve_column_expressions();
        } else {
          return shared_from_this();
        }
      } else {
        return referenced_expression->deep_copy()->deep_resolve_column_expressions();
      }
    } else {
      Fail("Resolving ColumnExpressions only possible in LQP");
    }
  }
  /**@}*/

  ColumnReference column;
};

using LQPColumnExpression = ColumnExpression<LQPColumnReference>;
using PQPColumnExpression = ColumnExpression<ColumnID>;

}  // namespace opossum
