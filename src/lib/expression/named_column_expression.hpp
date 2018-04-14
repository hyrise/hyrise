#pragma once

#include "abstract_column_expression.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

class NamedColumnExpression : public AbstractColumnExpression {
 public:
  explicit NamedColumnExpression(const ColumnIdentifier& qualified_column_name);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

  ColumnIdentifier qualified_column_name;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
