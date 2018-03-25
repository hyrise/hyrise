#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class FunctionType {
  Substring, Extract
};

class FunctionExpression : public AbstractExpression {
 public:
  FunctionExpression(const FunctionType function_type,
                      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;

  FunctionType function_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

} // namespace opossum
