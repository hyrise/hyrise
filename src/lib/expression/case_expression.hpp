#pragma once

#include <optional>

#include <boost/variant.hpp>

#include "abstract_expression.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

namespace hyrise {

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

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace hyrise
