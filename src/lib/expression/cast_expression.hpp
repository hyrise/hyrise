#pragma once

#include <memory>

#include "abstract_expression.hpp"

namespace hyrise {

/**
 * SQL's CAST
 * NOT a FunctionExpression since we currently have no way for taking as an enum such as DataType as a function
 * argument
 */
class CastExpression : public AbstractExpression {
 public:
  CastExpression(const std::shared_ptr<AbstractExpression>& argument, const DataType data_type);

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  std::shared_ptr<AbstractExpression> argument() const;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;

 private:
  const DataType _data_type;
};

}  // namespace hyrise
