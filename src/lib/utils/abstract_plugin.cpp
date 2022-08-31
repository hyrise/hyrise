#include "abstract_plugin.hpp"

namespace hyrise {

// We have to instantiate this function here because clang-12(+) does not instantiate it and llvm-cov throws a warning
// (functions have mismatched data). See
// https://stackoverflow.com/questions/57331600/llvm-cov-statistics-for-uninstantiated-functions
std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> AbstractPlugin::provided_user_executable_functions() {
  return {};
}

}  // namespace hyrise
