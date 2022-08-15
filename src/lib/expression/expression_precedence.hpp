#pragma once

#include <stdint.h>

namespace hyrise {

// Precedence levels for parenthesizing expression arguments. See AbstractExpression::_enclose_argument
enum class ExpressionPrecedence : uint32_t {
  Highest = 0,
  UnaryPredicate,
  MultiplicationDivision,
  AdditionSubtraction,
  BinaryTernaryPredicate,
  Logical
};

}  // namespace hyrise
