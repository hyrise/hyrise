#pragma once

#include "abstract_select_expression.hpp"
#include "all_type_variant.hpp"
#include "expression/parameter_expression.hpp"

namespace opossum {

class AbstractOperator;

class PQPSelectExpression : public AbstractSelectExpression {
 public:
  using Parameters = std::vector<std::pair<ParameterID, ColumnID>>;

  PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp,
                               const DataType data_type,
                               const bool nullable,
                               const Parameters& parameters);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const std::shared_ptr<AbstractOperator> pqp;
  const Parameters parameters;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const DataType _data_type;
  const bool _nullable;
};

}  // namespace opossum
