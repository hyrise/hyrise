#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class ExistsExpressionType { Exists, NotExists };

/**
 * SQL's EXISTS()
 */
class ExistsExpression : public AbstractExpression {
 public:
  ExistsExpression(const std::shared_ptr<AbstractExpression>& select,
                   const ExistsExpressionType exists_expression_type);

  std::shared_ptr<AbstractExpression> select() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const ExistsExpressionType exists_expression_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
