#pragma once

#include "abstract_expression.hpp"

namespace opossum {

// For a possible list of functions, see https://www.w3schools.com/sql/sql_ref_sqlserver.asp
enum class FunctionType {
  Substring,   // SUBSTR()
  Concatenate  // CONCAT()
};

class FunctionExpression : public AbstractExpression {
 public:
  FunctionExpression(const FunctionType function_type,
                     const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  FunctionType function_type;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
};

}  // namespace opossum
