#pragma once

#include <boost/preprocessor/stringize.hpp>

#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

#include "invalid_input_exception.hpp"

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

// __FILENAME__ is __FILE__ with irrelevant leading chars trimmed
#ifndef __FILENAME__
#define __FILENAME__ (__FILE__ + SOURCE_PATH_SIZE)
#endif

namespace opossum {

[[noreturn]] inline void Fail(const std::string& msg) { throw std::logic_error(msg); }

[[noreturn]] inline void FailInput(const std::string& msg) {
  throw InvalidInputException(std::string("Invalid input error: ") + msg);
}

}  // namespace opossum

#define Assert(expr, msg)                                                                  \
  if (!static_cast<bool>(expr)) {                                                          \
    opossum::Fail(std::string(__FILENAME__) + ":" BOOST_PP_STRINGIZE(__LINE__) " " + msg); \
  }

#define AssertInput(expr, msg)                                               \
  if (!static_cast<bool>(expr)) {                                            \
    throw InvalidInputException(std::string("Invalid input error: ") + msg); \
  }

#if IS_DEBUG
#define DebugAssert(expr, msg) Assert(expr, msg)
#else
#define DebugAssert(expr, msg)
#endif
