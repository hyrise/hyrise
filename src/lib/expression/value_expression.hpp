#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace opossum {

class ValueExpression : public AbstractExpression {
 public:
  explicit ValueExpression(const AllTypeVariant& value);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;
  
  AllTypeVariant value;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
