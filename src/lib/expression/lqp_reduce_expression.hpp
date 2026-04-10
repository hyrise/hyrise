#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace hyrise {

class AbstractLQPNode;
class LQPColumnExpression;

/**
 * A PQPReduceExpression represents a subquery (think `a > (SELECT MIN(a) FROM ...`) used as part of an expression in
 * a PQP.
 *
 * The parameters of a PQPSubqueryExpression are equivalent to the correlated parameters of a nested SELECT in SQL.
 */
class LQPReduceExpression : public AbstractExpression {
 public:
  // Constructor for single-column PQPSubqueryExpressions as used in `a IN (SELECT ...)` or `SELECT (SELECT ...)`.
  LQPReduceExpression(const std::shared_ptr<LQPColumnExpression>& reduced_column,
                      const std::shared_ptr<AbstractLQPNode>& reducer);

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;

  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  std::shared_ptr<LQPColumnExpression> reduced_column() const;
  std::shared_ptr<AbstractLQPNode> reducer() const;

 protected:
  // We do not implement `_shallow_hash()`, forcing a hash collision for LQPReduceExpression and triggering a full
  // equality check. In practice, LQPReduceExpressions should not appear in regular plan hashes, anyways.
  bool _shallow_equals(const AbstractExpression& expression) const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const override;

  std::weak_ptr<AbstractLQPNode> _reducer;
};

}  // namespace hyrise
