#include "performance_warning.hpp"

#include <iostream>

namespace opossum {

PerformanceWarningClass::PerformanceWarningClass(const std::string& text) {
  if (_disabled) return;
  std::cerr << "[PERF] " << text << "\n\tPerformance can be affected. This warning is only shown once.\n" << std::endl;
}

bool PerformanceWarningClass::disable() {
  bool previous = _disabled;
  _disabled = true;
  return previous;
}

void PerformanceWarningClass::enable() { _disabled = false; }

bool PerformanceWarningClass::_disabled = []() {  // NOLINT
// static initializer hack to print some warnings in various binaries

#if HYRISE_DEBUG
  PerformanceWarning("Hyrise is running as a debug build.");
#endif

  return false;
}();

}  // namespace opossum
