#pragma once

#include <exception>
#include <string>

/**
 * This file provides better assertions than the std cassert/assert.h - DebugAssert(condition, msg) and Fail(msg) can be
 * used
 * to both harden code by programming by contract and document the invariants enforced in messages.
 *
 * --> Use DebugAssert() whenever a certain invariant must hold, as in
 *
 * int divide(int numerator, int denonimator) {
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
 *     default: DebugFail("Illegal paramemeter");
 * }
 *
 * --> Use Assert() whenever an invariant should be checked even in release builds, either because testing it is
 *     very cheap or the invariant is considered very important
 */

namespace opossum {

inline void Assert(bool expr, const std::string& msg) {
  if (expr) {
    return;
  }
  throw std::logic_error(msg);
}

inline void Fail(const std::string& msg) { throw std::logic_error(msg); }

#if IS_DEBUG

inline void DebugAssert(bool expr, const std::string& msg) { Assert(expr, msg); }

inline void DebugFail(const std::string& msg) { Fail(msg); }

#else

inline void DebugAssert(bool expr, const std::string& msg) {}

inline void DebugFail(const std::string& msg) {}

#endif

}  // namespace opossum
