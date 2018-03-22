#pragma once

#include "abstract_expression.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

template<typename ColumnReference>
class ColumnExpression : public AbstractExpression {
  explicit ColumnExpression(const ColumnReference& column);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/

  ColumnReference column;
};

using LQPColumnExpression = ColumnExpression<LQPColumnReference>;
using PQPColumnExpression = ColumnExpression<ColumnID>;

}  // namespace opossum
