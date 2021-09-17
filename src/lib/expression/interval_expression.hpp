#pragma once

#include <memory>

#include "abstract_expression.hpp"

namespace opossum {

/**
 * SQL's INTERVAL
 */
class IntervalExpression : public AbstractExpression {
 public:
  IntervalExpression(const int64_t duration, const DatetimeComponent unit);

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;

  DataType data_type() const override;

  int64_t duration() const;

  DatetimeComponent unit() const;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;

 private:
  const int64_t _duration;
  const DatetimeComponent _unit;
};

}  // namespace opossum
