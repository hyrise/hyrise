#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class AbstractOperator;

class PQPSelectExpression : public AbstractExpression {
 public:
  explicit PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp,
                               const DataType data_type,
                               const bool nullable,
                               const std::vector<ColumnID>& parameters);

  bool requires_calculation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  std::shared_ptr<AbstractOperator> pqp;
  std::vector<ColumnID> parameters;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const DataType _data_type;
  const bool _nullable;
};

}  // namespace opossum
