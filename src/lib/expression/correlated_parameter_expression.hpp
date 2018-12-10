#pragma once

#include "abstract_parameter_expression.hpp"

namespace opossum {

/**
 * Represents an external value in a correlated sub select
 * (e.g. `extern.x` in `SELECT (SELECT MIN(a) WHERE a > extern.x) FROM extern`).
 *
 * Does NOT contain a shared_ptr to the expression it references since that would make LQP/PQP/Expression deep_copy()ing
 * extremely hard. Instead, it extracts all information it needs from the referenced expression into
 * ReferencedExpressionInfo
 */
class CorrelatedParameterExpression : public AbstractParameterExpression {
 public:
  // If this ParameterExpression is
  struct ReferencedExpressionInfo {
    ReferencedExpressionInfo(const DataType data_type, const bool nullable, const std::string& column_name);

    bool operator==(const ReferencedExpressionInfo& rhs) const;

    DataType data_type;
    bool nullable;
    std::string column_name;
  };

  CorrelatedParameterExpression(const ParameterID parameter_id, const AbstractExpression& referenced_expression);
  CorrelatedParameterExpression(const ParameterID parameter_id, const ReferencedExpressionInfo& referenced_expression_info);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const ReferencedExpressionInfo _referenced_expression_info;
};

}  // namespace opossum
