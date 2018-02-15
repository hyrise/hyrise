#pragma once

#include <types.hpp>

namespace opossum {

template <typename T>
std::function<T(const T&, const T&)> _get_base_operator_function(ExpressionType type) {
  switch (type) {
    case ExpressionType::Addition:
      return std::plus<T>();
    case ExpressionType::Subtraction:
      return std::minus<T>();
    case ExpressionType::Multiplication:
      return std::multiplies<T>();
    case ExpressionType::Division:
      return std::divides<T>();

    default:
      Fail("Unknown arithmetic operator");
  }
}

template <typename T>
std::function<T(const T&, const T&)> function_for_arithmetic_expression(ExpressionType type) {
  if (type == ExpressionType::Modulo) return std::modulus<T>();
  return _get_base_operator_function<T>(type);
}

/**
 * Specialized arithmetic operator implementation for std::string.
 * Two string terms can be added. Anything else is undefined.
 *
 * @returns a lambda function to solve arithmetic string terms
 *
 */
template <>
inline std::function<std::string(const std::string&, const std::string&)> function_for_arithmetic_expression(
    ExpressionType type) {
  Assert(type == ExpressionType::Addition, "Arithmetic operator except for addition not defined for std::string");
  return std::plus<std::string>();
}

/**
 * Specialized arithmetic operator implementation for float/double
 * Modulo on float isn't defined.
 *
 * @returns a lambda function to solve arithmetic float/double terms
 *
 */
template <>
inline std::function<float(const float&, const float&)> function_for_arithmetic_expression(ExpressionType type) {
  return _get_base_operator_function<float>(type);
}

template <>
inline std::function<double(const double&, const double&)> function_for_arithmetic_expression(ExpressionType type) {
  return _get_base_operator_function<double>(type);
}

/**
 * Specialized arithmetic operator implementation for int
 * Division by 0 needs to be caught when using integers.
 */
template <>
inline std::function<int(const int&, const int&)> function_for_arithmetic_expression(ExpressionType type) {
  if (type == ExpressionType::Division) {
    return [](const int& lhs, const int& rhs) {
      if (rhs == 0) {
        throw std::runtime_error("Cannot divide integers by 0.");
      }
      return lhs / rhs;
    };
  }
  return _get_base_operator_function<int>(type);
}

}  // namespace opossum
