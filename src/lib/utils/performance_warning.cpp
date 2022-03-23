#include "performance_warning.hpp"

namespace opossum {

bool PerformanceWarningClass::_disabled = []() {  // NOLINT
  // static initializer hack to print some warnings in various binaries

  if constexpr (HYRISE_DEBUG) {
    std::cout << "Note: Hyrise is running as a debug build. Performance may be affected." << std::endl;
  }

  return false;
}();

}  // namespace opossum
