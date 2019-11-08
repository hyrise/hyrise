#pragma once

#include <iostream>
#include <string>

#include <boost/preprocessor/stringize.hpp>

#include "utils/string_utils.hpp"

/**
 * Performance Warnings can be used in places where slow workarounds are used. This includes BaseSegment[] or the
 * use of a cross join followed by a projection instead of an equijoin.
 *
 * The warnings are printed only once per program execution. This is achieved by using static variables.
 *
 * Performance warnings can be disabled using the RAII-style PerformanceWarningDisabler:
 *
 * {
 *   PerformanceWarningDisabler pwd;
 *   std::cout << base_segment[5] << std::endl; // this does not cause a warning
 * }
 * // warnings are enabled again
 *
 * Warnings do not print in tests.
 */

namespace opossum {

class PerformanceWarningDisabler;

class PerformanceWarningClass {
 public:
  explicit PerformanceWarningClass(const std::string& text) {
    if (_disabled) return;
    std::cerr << "[PERF] " << text << "\n\tPerformance can be affected. This warning is only shown once.\n"
              << std::endl;
  }

 protected:
  static bool _disabled;

  static bool disable() {
    bool previous = _disabled;
    _disabled = true;
    return previous;
  }

  static void enable() { _disabled = false; }

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

#define PerformanceWarning(text)                                                                       \
  {                                                                                                    \
    static PerformanceWarningClass warn(std::string(text) + " at " + trim_source_file_path(__FILE__) + \
                                        ":" BOOST_PP_STRINGIZE(__LINE__));                             \
  }                                                                                                    \
  static_assert(true, "End call of macro with a semicolon")

}  // namespace opossum
