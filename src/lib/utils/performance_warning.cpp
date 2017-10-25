#include "performance_warning.hpp"

namespace opossum {

bool PerformanceWarningClass::_disabled = []() {
// static initalizer hack to print some warnings in various binaries

#if !IS_DEBUG && !defined(WITH_LTO)
  PerformanceWarning("Hyrise was built without Link-Time Optimization - update to cmake >= 3.9");
#endif

#if IS_DEBUG
  PerformanceWarning("Hyrise is running as a debug build.");
#endif

  return false;
}();

}  // namespace opossum
