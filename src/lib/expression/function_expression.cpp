#include "function_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "constant_mappings.hpp"
#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

FunctionExpression::FunctionExpression(const FunctionType function_type,
                                       const std::vector<std::shared_ptr<AbstractExpression>>& arguments)
    : AbstractExpression(ExpressionType::Function, arguments), function_type(function_type) {
  switch (function_type) {
    case FunctionType::Substring:
      Assert(arguments.size() == 3, "Substring expects 3 parameters");
      break;
    case FunctionType::Concatenate:
      Assert(arguments.size() >= 2, "Concatenate expects at least 2 parameters");
      for (const auto& argument : arguments) {
        Assert(argument->data_type() == DataType::String || argument->data_type() == DataType::Null,
               "Concatenate takes only Strings and Nulls as arguments");
      }
      break;
  }
}

std::shared_ptr<AbstractExpression> FunctionExpression::deep_copy() const {
  return std::make_shared<FunctionExpression>(function_type, expressions_deep_copy(arguments));
}

std::string FunctionExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << function_type << "(";
  for (auto argument_idx = size_t{0}; argument_idx < arguments.size(); ++argument_idx) {
    stream << arguments[argument_idx]->description(mode);
    if (argument_idx + 1 < arguments.size()) stream << ",";
  }
  stream << ")";
  return stream.str();
}

DataType FunctionExpression::data_type() const {
  switch (function_type) {
    case FunctionType::Substring:
    case FunctionType::Concatenate:
      return DataType::String;
  }
  Fail("Invalid enum value");
}

bool FunctionExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const FunctionExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");

  const auto& function_expression = static_cast<const FunctionExpression&>(expression);
  return function_type == function_expression.function_type &&
         expressions_equal(arguments, function_expression.arguments);
}

size_t FunctionExpression::_shallow_hash() const { return boost::hash_value(static_cast<size_t>(function_type)); }

}  // namespace opossum
