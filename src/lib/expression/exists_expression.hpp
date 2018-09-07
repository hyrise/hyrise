#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * SQL's EXISTS()
 */
class ExistsExpression : public AbstractExpression {
 public:
  explicit ExistsExpression(const std::shared_ptr<AbstractExpression>& select);

  std::shared_ptr<AbstractExpression> select() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
