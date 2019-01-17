#pragma once

#include "abstract_expression.hpp"  // NEEDEDINCLUDE

namespace opossum {

/**
 * Named after SQL's CASE.
 * To build a Case with more then one WHEN clause, nest additional CaseExpressions into the otherwise branch.
 */
class CaseExpression : public AbstractExpression {
 public:
  CaseExpression(const std::shared_ptr<AbstractExpression>& when, const std::shared_ptr<AbstractExpression>& then,
                 const std::shared_ptr<AbstractExpression>& otherwise);

  const std::shared_ptr<AbstractExpression>& when() const;
  const std::shared_ptr<AbstractExpression>& then() const;
  const std::shared_ptr<AbstractExpression>& otherwise() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
