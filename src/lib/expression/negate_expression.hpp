#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class NegateExpression : public AbstractExpression {
 public:
  NegateExpression(const std::shared_ptr<AbstractExpression>& argument);
  
  std::shared_ptr<AbstractExpression> argument() const;
  
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override; 
};

}  // namespace opossum
