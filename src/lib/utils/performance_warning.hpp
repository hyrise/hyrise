#pragma once

#include <boost/preprocessor/stringize.hpp>
#include <iostream>
#include <string>

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
    static bool _disabled{false};
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
