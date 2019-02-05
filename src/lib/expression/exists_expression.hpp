#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class ExistsExpressionType { Exists, NotExists };

/**
 * SQL's EXISTS()
 */
class ExistsExpression : public AbstractExpression {
 public:
  ExistsExpression(const std::shared_ptr<AbstractExpression>& subquery,
                   const ExistsExpressionType exists_expression_type);

  std::shared_ptr<AbstractExpression> subquery() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  const ExistsExpressionType exists_expression_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
};

}  // namespace opossum
