#include "abstract_plugin.hpp"

#include <optional>
#include <utility>
#include <vector>

namespace hyrise {

// We have to instantiate this function here because clang-12(+) does not instantiate it and llvm-cov throws a warning
// (functions have mismatched data). See
// https://stackoverflow.com/questions/57331600/llvm-cov-statistics-for-uninstantiated-functions
std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> AbstractPlugin::provided_user_executable_functions() {
  return {};
}

std::optional<PreBenchmarkHook> AbstractPlugin::pre_benchmark_hook() {
  return {};
}

std::optional<PostBenchmarkHook> AbstractPlugin::post_benchmark_hook() {
  return {};
}

}  // namespace hyrise
