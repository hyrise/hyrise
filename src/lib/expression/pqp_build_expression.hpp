#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace hyrise {

class Build;

/**
 * A PQPSubqueryExpression represents a subquery (think `a > (SELECT MIN(a) FROM ...`) used as part of an expression in
 * a PQP.
 *
 * The parameters of a PQPSubqueryExpression are equivalent to the correlated parameters of a nested SELECT in SQL.
 */
class PQPBuildExpression : public AbstractExpression {
 public:
  // Constructor for single-column PQPSubqueryExpressions as used in `a IN (SELECT ...)` or `SELECT (SELECT ...)`.
  PQPBuildExpression(const std::shared_ptr<Build>& init_pqp, const ColumnID init_column_id, const DataType data_type);

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  std::string description(const DescriptionMode /*mode*/) const override;
  DataType data_type() const override;

  const std::shared_ptr<Build> build;
  const ColumnID column_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const override;

  DataType _data_type;
};

}  // namespace hyrise
