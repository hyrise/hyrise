#include "plugin_test_utils.hpp"

namespace opossum {

#ifdef __APPLE__
#define DYNAMIC_LIBRARY_SUFFIX ".dylib"
#elif __linux__
#define DYNAMIC_LIBRARY_SUFFIX ".so"
#endif

const std::string build_dylib_path(const std::string& name) {
  // CMAKE makes TEST_PLUGIN_DIR point to the ${CMAKE_BINARY_DIR}/lib/
  // Dynamic libraries have platform-dependent suffixes
  return std::string(TEST_PLUGIN_DIR) + name + std::string(DYNAMIC_LIBRARY_SUFFIX);
}

}  // namespace opossum
