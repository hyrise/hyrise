#include "function_expression.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression_utils.hpp"
#include "operators/abstract_operator.hpp"
#include "utils/assert.hpp"

namespace hyrise {

FunctionExpression::FunctionExpression(const FunctionType init_function_type,
                                       const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments)
    : AbstractExpression(ExpressionType::Function, init_arguments), function_type(init_function_type) {
  switch (function_type) {
    case FunctionType::Substring:
      Assert(arguments.size() == 3, "Substring expects three arguments.");
      Assert(arguments[0]->data_type() == DataType::String || arguments[0]->data_type() == DataType::Null,
             "Substring expects an expression with data type String or Null as first argument.");
      for (auto argument_idx = size_t{1}; argument_idx <= size_t{2}; ++argument_idx) {
        const auto data_type = arguments[argument_idx]->data_type();
        Assert(data_type != DataType::String && !is_floating_point_data_type(data_type),
               "Substring expects expressions with data type Integer, Long, or Null as second and third argument.");
      }
      break;
    case FunctionType::Concatenate:
      Assert(arguments.size() >= 2, "Concatenate expects at least two arguments.");
      for (const auto& argument : arguments) {
        Assert(argument->data_type() == DataType::String || argument->data_type() == DataType::Null,
               "Concatenate takes only Strings and Nulls as arguments.");
      }
      break;
    case FunctionType::Absolute:
      Assert(arguments.size() == 1, "Absolute expects exactly one argument.");
      Assert(arguments[0]->data_type() != DataType::String, "Absolute is not defined on Strings.");
  }
}

std::shared_ptr<AbstractExpression> FunctionExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<FunctionExpression>(function_type, expressions_deep_copy(arguments, copied_ops));
}

std::string FunctionExpression::description(const DescriptionMode mode) const {
  auto stream = std::stringstream{};

  stream << function_type << "(";
  for (auto argument_idx = size_t{0}; argument_idx < arguments.size(); ++argument_idx) {
    stream << arguments[argument_idx]->description(mode);
    if (argument_idx + 1 < arguments.size()) {
      stream << ",";
    }
  }
  stream << ")";
  return stream.str();
}

DataType FunctionExpression::data_type() const {
  switch (function_type) {
    case FunctionType::Substring:
    case FunctionType::Concatenate:
      return DataType::String;
    case FunctionType::Absolute:
      return arguments.front()->data_type();
  }
  Fail("Invalid enum value.");
}

bool FunctionExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const FunctionExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");

  const auto& function_expression = static_cast<const FunctionExpression&>(expression);
  return function_type == function_expression.function_type &&
         expressions_equal(arguments, function_expression.arguments);
}

size_t FunctionExpression::_shallow_hash() const {
  return std::hash<FunctionType>{}(function_type);
}

std::ostream& operator<<(std::ostream& stream, const FunctionType function_type) {
  return stream << function_type_to_string.left.at(function_type);
}

}  // namespace hyrise
