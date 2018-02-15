#pragma once

#include "arithmetic_operator_expression_impl.hpp"

#include "types.hpp"

namespace opossum {

/**
 * Returns the arithmetic operator function equivalent to the given ExpressionType.
 */
template <typename T>
std::function<T(const T&, const T&)> function_for_arithmetic_expression(ExpressionType type);

}  // namespace opossum
