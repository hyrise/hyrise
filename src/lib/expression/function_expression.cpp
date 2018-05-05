#include "function_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "utils/assert.hpp"
#include "expression_utils.hpp"

namespace opossum {

FunctionExpression::FunctionExpression(const FunctionType function_type,
                                         const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
AbstractExpression(ExpressionType::Function, arguments) {

  switch (function_type) {
    case FunctionType::Substring: Assert(arguments.size() == 3, "SUBSTRING expects 3 parameters"); break;
  }
}

std::shared_ptr<AbstractExpression> FunctionExpression::deep_copy() const {
  return std::make_shared<FunctionExpression>(function_type, expressions_copy(arguments));
}

std::string FunctionExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

DataType FunctionExpression::data_type() const {
  switch (function_type) {
    case FunctionType::Substring: return DataType::String;
  }
}

bool FunctionExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& function_expression = static_cast<const FunctionExpression&>(expression);
  return function_type == function_expression.function_type &&
    expressions_equal(arguments, function_expression.arguments);
}

size_t FunctionExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(function_type));
}

}  // namespace opossum
