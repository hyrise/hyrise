#pragma once

#include <memory>
#include <string>

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"
#include "strong_typedef.hpp"

/**
 * Used to identify a Parameter within a (Sub)Select. This can be either a parameter of a Prepared SELECT statement
 * `SELECT * FROM t WHERE a > ?` or a correlated parameter in a Subselect.
 */
STRONG_TYPEDEF(size_t, ParameterID);

namespace opossum {

enum class ParameterExpressionType { ValuePlaceholder, External };

/**
 * Represents a value placeholder (SELECT a + ? ...) or an external value in a correlated sub select
 * (e.g. `extern.x` in `SELECT (SELECT MIN(a) WHERE a > extern.x) FROM extern`).
 *
 * If it is a value placeholder no type info/nullable info/column name is available.
 *
 * Does NOT contain a shared_ptr to the expression it references since that would make LQP/PQP/Expression deep_copy()ing
 * extremely hard. Instead, it extracts all information it needs from the referenced expression into
 * ReferencedExpressionInfo
 */
class ParameterExpression : public AbstractExpression {
 public:
  // If this ParameterExpression is
  struct ReferencedExpressionInfo {
    ReferencedExpressionInfo(const DataType data_type, const bool nullable, const std::string& column_name);

    bool operator==(const ReferencedExpressionInfo& rhs) const;

    DataType data_type;
    bool nullable;
    std::string column_name;
  };

  // Constructs a value placeholder
  explicit ParameterExpression(const ParameterID parameter_id);

  // Constructs an external value
  ParameterExpression(const ParameterID parameter_id, const AbstractExpression& referenced_expression);
  ParameterExpression(const ParameterID parameter_id, const ReferencedExpressionInfo& referenced_expression_info);

  bool requires_computation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const std::optional<AllTypeVariant>& value() const;
  void set_value(const std::optional<AllTypeVariant>& value);

  const ParameterID parameter_id;
  const ParameterExpressionType parameter_expression_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const std::optional<ReferencedExpressionInfo> _referenced_expression_info;

  // Value placeholder: Get's set once during EXECUTE <prepared-statement>
  // External value: Get's set (multiple times) in AbstractOperator::set_parameter during expression execution
  std::optional<AllTypeVariant> _value;
};

}  // namespace opossum
