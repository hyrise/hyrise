#pragma once

#include "abstract_column_expression.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

class LQPColumnExpression : public AbstractColumnExpression {
 public:
  explicit LQPColumnExpression(const QualifiedColumnName& qualified_column_name,
                               const std::optional<LQPColumnReference>& column_reference = std::nullopt);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;

  QualifiedColumnName qualified_column_name;
  std::optional<LQPColumnReference> column_reference;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
