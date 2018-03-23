#pragma once

#include <optional>

#include "boost/variant.hpp"

#include "abstract_expression.hpp"
#include "null_expression.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @defgroup Types to describe Case's similar to SQL's CASE expression.
 * Terms are kept similar to SQL92.
 */

struct CaseWhenClause final {
  CaseWhenClause(const std::shared_ptr<AbstractExpression>& when, const std::shared_ptr<AbstractExpression>& then) : when(when), then(then) {}

  std::shared_ptr<AbstractExpression> when;
  std::shared_ptr<AbstractExpression> then;
};

class CaseExpression : public AbstractExpression {
  explicit CaseExpression(const CaseWhenClause& clause, const std::shared_ptr<AbstractExpression>& else_ = std::make_shared<NullExpression>());
  explicit CaseExpression(const std::vector<CaseWhenClause>& clauses, const std::shared_ptr<AbstractExpression>& else_ = std::make_shared<NullExpression>());

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  bool deep_equals(const AbstractExpression& expression) const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> deep_resolve_column_expressions() override;
  /**@}*/

  std::vector<CaseWhenClause> clauses;
  std::shared_ptr<AbstractExpression> else_;
};

/**@}*/

}  // namespace opossum
