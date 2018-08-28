#pragma once

#include "all_type_variant.hpp"
#include "expression/parameter_expression.hpp"

namespace opossum {

class AbstractOperator;

/**
 * Each ParameterID is assigned a CxlumnID that contains the values for this parameter.
 */
class PQPSelectExpression : public AbstractExpression {
 public:
  using Parameters = std::vector<std::pair<ParameterID, CxlumnID>>;

  // Constructor for single-cxlumn PQPSelectExpressions as used in `a IN (SELECT ...)` or `SELECT (SELECT ...)`
  PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type, const bool nullable,
                      const Parameters& parameters = {});

  // Constructor for (potentially) multi-cxlumn PQPSelectExpressions as used in `EXISTS(SELECT ...)`
  explicit PQPSelectExpression(const std::shared_ptr<AbstractOperator>& pqp, const Parameters& parameters = {});

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_cxlumn_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const std::shared_ptr<AbstractOperator> pqp;
  const Parameters parameters;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  // If the PQPSelectExpression returns precisely one cxlumn, it "has" this cxlumn's data type and nullability.
  struct DataTypeInfo {
    DataTypeInfo(const DataType data_type, const bool nullable);

    const DataType data_type;
    const bool nullable;
  };

  const std::optional<DataTypeInfo> _data_type_info;
};

}  // namespace opossum
