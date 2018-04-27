#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class AbstractOperator;

struct PQPSelectParameter final {
  ColumnID column_id{INVALID_COLUMN_ID};
  ValuePlaceholder value_placeholder{0};
};

class PQPSelectExpression : public AbstractExpression {
 public:
  explicit PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type_, const std::vector<PQPSelectParameter>& parameters);

  bool requires_calculation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  ExpressionDataTypeVariant data_type() const override;

  std::shared_ptr<AbstractOperator> pqp;
  std::vector<PQPSelectParameter> parameters;
  const DataType data_type_;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
