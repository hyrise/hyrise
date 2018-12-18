#include "performance_warning.hpp"

namespace opossum {

bool PerformanceWarningClass::_disabled = []() {  // NOLINT
// static initializer hack to print some warnings in various binaries

#if HYRISE_DEBUG
  PerformanceWarning("Hyrise is running as a debug build.");
#endif

  return false;
}();

}  // namespace opossum
