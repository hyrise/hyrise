#pragma once

#include <memory>
#include <string>

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

enum class ParameterExpressionType { Placeholder, Correlated };

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
class AbstractParameterExpression : public AbstractExpression {
 public:
  AbstractParameterExpression(const ParameterExpressionType parameter_expression_type, const ParameterID parameter_id);

  bool requires_computation() const override;

  const ParameterExpressionType parameter_expression_type;
  const ParameterID parameter_id;
};

}  // namespace opossum
