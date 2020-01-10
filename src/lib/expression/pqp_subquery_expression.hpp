#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class AbstractOperator;

/**
 * A PQPSubqueryExpression represents a subquery (think `a > (SELECT MIN(a) FROM ...`) used as part of an expression in
 * a PQP.
 *
 * The Parameters to a PQPSubqueryExpression are equivalent to the correlated parameters of a nested SELECT in SQL.
 */
class PQPSubqueryExpression : public AbstractExpression {
 public:
  using Parameters = std::vector<std::pair<ParameterID, ColumnID>>;

  // Constructor for single-column PQPSubqueryExpressions as used in `a IN (SELECT ...)` or `SELECT (SELECT ...)`
  PQPSubqueryExpression(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type, const bool nullable,
                        const Parameters& parameters = {});

  // Constructor for (potentially) multi-column PQPSubqueryExpressions as used in `EXISTS(SELECT ...)`
  explicit PQPSubqueryExpression(const std::shared_ptr<AbstractOperator>& pqp, const Parameters& parameters = {});

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  // Returns whether this query is correlated, i.e., uses external parameters
  bool is_correlated() const;

  const std::shared_ptr<AbstractOperator> pqp;
  const Parameters parameters;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;

 private:
  // If the PQPSubqueryExpression returns precisely one column, it "has" this column's data type and nullability.
  struct DataTypeInfo {
    DataTypeInfo(const DataType data_type, const bool nullable);

    const DataType data_type;
    const bool nullable;
  };

  const std::optional<DataTypeInfo> _data_type_info;
};

}  // namespace opossum
