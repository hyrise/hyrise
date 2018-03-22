#pragma once

#include <optional>

#include "boost/variant.hpp"

#include "abstract_expression.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @defgroup Types to describe Case's similar to SQL's CASE expression in PQPs and LQPs.
 * Terms are kept similar to SQL92.
 *
 * # Nesting with other expression types
 *    Hyrise's CaseOperator supports no computations in its CASE, WHEN, THEN or ELSE statements - these have to be
 *    performed in previous Operators, e.g. Projections, and then used in Case by referencing
 *    the WHEN-column/CaseExpressionResults to the Column where the result of the computation is stored. Also, there is
 *    no special construct for SQL's searched CASE, because internally it can be supported with the same algorithms as
 *    the normal CASE.
 *
 * # On the WHEN condition
 *    The WHEN conditions have to be pre-computed (again, using e.g. Projection) and stored in a int32 column.
 *    0 means false and anything else means true
 *
 * # Internal syntax
 *    From the perspective of the CaseOperator, a Case expression looks like this.
 *         [WHEN <column> THEN <case_expression_result>, ...] ELSE <case_expression_result; default=NULL>
 *    where <case_expression_result> is either Null, a Column or a constant value.
 *
 * # Template over AllTypeVariant
 *    CaseExpression, CaseWhenClause and CaseExpressionResult are templated over the ResultDataType - instead of using
 *    an AllTypeVariant in CaseExpressionResult - to statically ensure that all CaseWhenClauses/ElseClauses in a
 *    CaseExpression yield the same type (and thus a CaseExpression yields a value of the same type for all rows).
 */

// The result from a CaseExpression, can be either Null, a Column or a constant value
// For why this is templated over ResultDataType, see group comment above
template <typename ColumnReference, typename ResultDataType>
using CaseExpressionResult = boost::variant<Null, ColumnReference, ResultDataType>;

// For why this is templated over ResultDataType, see group comment above
template <typename ColumnReference, typename ResultDataType>
struct CaseWhenClause final {
  using ThenType = CaseExpressionResult<ColumnReference, ResultDataType>;

  CaseWhenClause(const ColumnReference& when, const ThenType& then) : when(when), then(then) {}

  ColumnReference when;
  ThenType then;
};

class AbstractCaseExpression : public AbstractExpression {
  explicit AbstractCaseExpression(const DataType result_data_type) : AbstractExpression(ExpressionType::Case), result_data_type(result_data_type) {}
  virtual ~AbstractCaseExpression() = default;

  const DataType result_data_type;
};

// For why this is templated over ResultDataType, see group comment above
template <typename ColumnReference, typename ResultDataType>
struct CaseExpression : public AbstractCaseExpression {
  using ElseType = CaseExpressionResult<ColumnReference, ResultDataType>;
  using ClauseType = CaseWhenClause<ColumnReference, ResultDataType>;

  explicit CaseExpression(const ClauseType& clause, const ElseType& else_ = Null{})
      : CaseExpression(std::vector<ClauseType>{clause}, else_) {}
  explicit CaseExpression(const std::vector<ClauseType>& clauses, const ElseType& else_ = Null{})
      : AbstractCaseExpression(data_type_from_type<ResultDataType>()),
        clauses(clauses),
        else_(else_) {}  // NOLINT - lint thinks else_(else_) is wrong

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  std::shared_ptr<AbstractExpression> deep_copy() const {

  }

  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/

  std::vector<ClauseType> clauses;
  ElseType else_;
};

/**@}*/

}  // namespace opossum
