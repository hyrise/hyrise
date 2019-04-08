#pragma once

#include <boost/preprocessor/stringize.hpp>

#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

#include "invalid_input_exception.hpp"
#include "string_utils.hpp"

/**
 * This file provides better assertions than the std cassert/assert.h - DebugAssert(condition, msg) and Fail(msg) can be
 * used
 * to both harden code by programming by contract and document the invariants enforced in messages.
 *
 * --> Use DebugAssert() whenever a certain invariant must hold, as in
 *
 * int divide(int numerator, int denominator) {
 *   DebugAssert(denominator == 0, "Divisions by zero are not allowed");
 *   return numerator / denominator;
 * }
 *
 *
 * --> Use Fail() whenever an illegal code path is taken. Especially useful for switch statements:
 *
 * void foo(int v) {
 *   switch(v) {
 *     case 0: //...
 *     case 3: //...
 *     case 17: //...
 *     default: Fail("Illegal parameter");
 * }
 *
 * --> Use Assert() whenever an invariant should be checked even in release builds, either because testing it is
 *     very cheap or the invariant is considered very important
 *
 * --> Use AssertInput() to check if the user input is correct. This provides a more specific error handling since an
 *     invalid input might want to be caught.
 */

namespace opossum {

[[noreturn]] inline void Fail(const std::string& msg) { throw std::logic_error(msg); }

[[noreturn]] inline void FailInput(const std::string& msg) {
  throw InvalidInputException(std::string("Invalid input error: ") + msg);
}

}  // namespace opossum

#define Assert(expr, msg)                                                                                 \
  if (!static_cast<bool>(expr)) {                                                                         \
    opossum::Fail(opossum::trim_source_file_path(__FILE__) + ":" BOOST_PP_STRINGIZE(__LINE__) " " + msg); \
  }                                                                                                       \
  static_assert(true, "End call of macro with a semicolon")

#define AssertInput(expr, msg)                                               \
  if (!static_cast<bool>(expr)) {                                            \
    throw InvalidInputException(std::string("Invalid input error: ") + msg); \
  }                                                                          \
  static_assert(true, "End call of macro with a semicolon")

#if HYRISE_DEBUG
#define DebugAssert(expr, msg) Assert(expr, msg)
#else
#define DebugAssert(expr, msg)
#endif
