#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class AbstractOperator;

struct PQPSelectParameter final {
  ColumnID column_id;
  std::shared_ptr<AllTypeVariant> value;
};

class PQPSelectExpression : public AbstractExpression {
 public:
  explicit PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp);

  bool requires_calculation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

  std::shared_ptr<AbstractOperator> pqp;

  std::vector<PQPSelectParameter> parameters;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
