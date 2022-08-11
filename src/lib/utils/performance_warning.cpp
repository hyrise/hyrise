#include "performance_warning.hpp"

namespace hyrise {

bool PerformanceWarningClass::_disabled = []() {  // NOLINT
  // static initializer hack to print some warnings in various binaries

  if constexpr (HYRISE_DEBUG) {
    PerformanceWarning("Hyrise is running as a debug build.");
  }

  return false;
}();

}  // namespace hyrise
