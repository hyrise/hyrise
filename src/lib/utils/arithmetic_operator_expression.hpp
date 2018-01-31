#pragma once

#include "arithmetic_operator_expression_impl.hpp"

#include "types.hpp"

namespace opossum {

  /**
   * Returns the std::function for and ExpressionType that is an arithmetic operator with both operands being the same type.
   */
  template <typename T>
  std::function<T(const T&, const T&)> arithmetic_operator_function_from_expression(ExpressionType type);

}  // namespace opossum
