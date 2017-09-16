#pragma once

#include <boost/preprocessor/stringize.hpp>

#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

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
 */

namespace opossum {

template <typename T>
inline void Assert(const T& value, const std::string& msg) {
  if (static_cast<bool>(value)) {
    return;
  }
  throw std::logic_error(msg);
}

inline void Fail(const std::string& msg) { throw std::logic_error(msg); }

}  // namespace opossum

#if IS_DEBUG
#define __FILENAME__ (__FILE__ + SOURCE_PATH_SIZE)
#define DebugAssert(expr, msg) \
  opossum::Assert(expr, std::string{__FILENAME__} + ":" BOOST_PP_STRINGIZE(__LINE__) " " + msg)  //  NOLINT
#else
#define DebugAssert(expr, msg)
#endif

/**
 * Performance Warnings can be used in places where slow workarounds are used. This includes BaseColumn[] or the
 * use of a cross join followed by a projection instead of an equijoin.
 *
 * The warnings are printed only once per program execution. This is achieved by using static variables.
 *
 * Performance warnings can be disabled using the RAII-style PerformanceWarningDisabler:
 *
 * {
 *   PerformanceWarningDisabler pwd;
 *   std::cout << base_column[5] << std::endl; // this does not cause a warning
 * }
 * // warnings are enabled again
 *
 * Warnings do not print in tests.
 */

class PerformanceWarningDisabler;

class PerformanceWarningClass {
  static bool& disabled() {
    // hacky hack that allows us to have state in a header file
    static bool _disabled;
    return _disabled;
  }

 public:
  explicit PerformanceWarningClass(const std::string &text) {
    if (PerformanceWarningClass::disabled()) return;
    std::cout << "[PERF] " << text
              << "\n\tPerformance can be affected. This warning is only shown once."
              << std::endl;
  }

 protected:
  static bool disable() {
    bool previous = PerformanceWarningClass::disabled();
    PerformanceWarningClass::disabled() = true;
    return previous;
  }

  static void enable() { PerformanceWarningClass::disabled() = false; }

  friend class PerformanceWarningDisabler;
};

class PerformanceWarningDisabler {
bool _previously_disabled;
 public:
  PerformanceWarningDisabler() : _previously_disabled(PerformanceWarningClass::disable()) {}
  ~PerformanceWarningDisabler() {
    if (!_previously_disabled) PerformanceWarningClass::enable();
  }
};

#if IS_DEBUG
#define PerformanceWarning(text)\
  {\
    static PerformanceWarningClass warn(std::string(text) + " at " +\
      std::string{__FILENAME__} + ":" BOOST_PP_STRINGIZE(__LINE__)); }  // NOLINT
#else
#define PerformanceWarning(text)
#endif
