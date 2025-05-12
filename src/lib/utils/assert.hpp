#pragma once

#include <stdexcept>
#include <string>

#include <boost/preprocessor/stringize.hpp>

#include "invalid_input_exception.hpp"
#include "utils/string_utils.hpp"  // NOLINT(misc-include-cleaner): used in macro.

/**
 * This file provides better assertions than the std cassert/assert.h - DebugAssert(condition, msg) and Fail(msg) can be
 * used to both harden code by programming by contract and document the invariants enforced in messages.
 *
 * --> Use DebugAssert() whenever a certain invariant must hold, as in:
 *
 * int divide(int numerator, int denominator) {
 *   DebugAssert(denominator != 0, "Divisions by zero are not allowed.");
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
 *     default: Fail("Illegal parameter.");
 * }
 *
 * --> Use Assert() whenever an invariant should be checked even in release builds, either because testing it is very
 *     cheap or the invariant is considered very important.
 *
 * --> Use AssertInput() to check if the user input is correct. This provides a more specific error handling since an
 *     invalid input might want to be caught.
 */

namespace hyrise {

namespace detail {

// We need this indirection so that we can throw exceptions from destructors without the compiler complaining. This is
// generally forbidden and might lead to std::terminate, but as we do not want to handle most errors anyway, this is
// fine.
[[noreturn]] inline void fail(const std::string& msg) {
  throw std::logic_error(msg);
}

}  // namespace detail

[[noreturn]] inline void Fail(const std::string& msg) {  // NOLINT(readability-identifier-naming)
  hyrise::detail::fail(hyrise::trim_source_file_path(__FILE__) + ":" BOOST_PP_STRINGIZE(__LINE__) " " + msg);
}

[[noreturn]] inline void FailInput(const std::string& msg) {  // NOLINT(readability-identifier-naming)
  throw InvalidInputException(std::string("Invalid input error: ") + msg);
}

}  // namespace hyrise

template <typename E>
constexpr void Assert(const E& expr, const std::string& msg) {  // NOLINT(readability-identifier-naming)
  if (!static_cast<bool>(expr)) {
    hyrise::Fail(msg);
  }
}

template <typename E>
constexpr void AssertInput(const E& expr, const std::string& msg) {  // NOLINT(readability-identifier-naming)
  if (!static_cast<bool>(expr)) {
    hyrise::FailInput(msg);
  }
}

#if HYRISE_DEBUG
template <typename E>
constexpr void DebugAssert(const E& expr, const std::string& msg) {  // NOLINT(readability-identifier-naming)
  Assert(expr, msg);
}
#else
template <typename E>
constexpr void DebugAssert(const E& expr, const std::string& msg) {}  // NOLINT(readability-identifier-naming)
#endif
