#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * Represents an external value in a correlated subquery
 * (e.g. `extern.x` in `SELECT (SELECT MIN(a) WHERE a > extern.x) FROM extern`).
 *
 * Does NOT contain a shared_ptr to the expression it references since that would make LQP/PQP/Expression deep_copy()ing
 * extremely cumbersome to implement. Instead, it extracts all information it needs from the referenced expression into
 * ReferencedExpressionInfo
 *
 * NOTE: It is assumed that all correlated expression are nullable - it is very taxing,
 *       code-wise, to determine whether it actually is.
 */
class CorrelatedParameterExpression : public AbstractExpression {
 public:
  struct ReferencedExpressionInfo {
    ReferencedExpressionInfo(const DataType data_type, const std::string& column_name);

    bool operator==(const ReferencedExpressionInfo& rhs) const;

    DataType data_type;
    std::string column_name;
  };

  CorrelatedParameterExpression(const ParameterID parameter_id, const AbstractExpression& referenced_expression);
  CorrelatedParameterExpression(const ParameterID parameter_id,
                                const ReferencedExpressionInfo& referenced_expression_info);

  const std::optional<AllTypeVariant>& value() const;
  void set_value(const std::optional<AllTypeVariant>& value);

  bool requires_computation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  const ParameterID parameter_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;

 private:
  const ReferencedExpressionInfo _referenced_expression_info;

  // Gets set in AbstractOperator::set_parameter during expression execution
  std::optional<AllTypeVariant> _value;
};

}  // namespace opossum
