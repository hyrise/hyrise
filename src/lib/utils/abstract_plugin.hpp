#pragma once

#include <iostream>

#include "nlohmann/json.hpp"

#include "types.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

class AbstractBenchmarkItemRunner;

// This is necessary to make the plugin instantiable, it leads to plain C linkage to avoid ugly mangled names. Use
// EXPORT in the implementation file of your plugin.
// clang-format off
#define EXPORT_PLUGIN(PluginName)                           \
  extern "C" AbstractPlugin* factory() {                    \
    return new PluginName();                                \
  }                                                         \
  static_assert(true, "End call of macro with a semicolon")
// clang-format on

using PluginFunctionName = std::string;
using PluginFunctionPointer = std::function<void(void)>;
using PreBenchmarkHook = std::function<void(AbstractBenchmarkItemRunner&)>;
using PostBenchmarkHook = std::function<void(nlohmann::json& report)>;

// AbstractPlugin is the abstract super class for all plugins. An example implementation can be found under
// test/utils/test_plugin.cpp. Usually plugins are implemented as singletons because there should not be multiple
// instances of them as they would compete against each other.
class AbstractPlugin {
 public:
  virtual ~AbstractPlugin() = default;

  virtual std::string description() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;

  // This method provides an interface for making plugin functions executable by plugin users. Please see the
  // test_plugin.cpp and the meta_exec_table_test.cpp for examples on how to use it. IMPORTANT: The execution of such
  // user-executable functions is blocking, see the PluginManager's exec_user_function method. If you are writing a
  // plugin and provide user-exectuable functions it is YOUR responsibility to keep these function calls as short and
  // efficient as possible, e.g., by spinning up a thread inside the plugin to execute the actual functionality.
  virtual std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions();

  // Provides an interface to execute plugin functionality before the BenchmarkRunner executes the items. This can be
  // used to, e.g., let workload-driven advisors execute the workload and apply their suggestions. Thus, the
  // pre-benchmark hook gets the BenchmarkRunner's _benchmark_item_runner as an argument to access the workload's
  // queries. For an example, see the implementation in `dependency_discovery_plugin.cpp`.
  virtual std::optional<PreBenchmarkHook> pre_benchmark_hook();

  // Provides an interface to execute plugin functionality after the BenchmarkRunner executed the items. We can use this
  // to, e.g., export logs or workload information. The post-benchmark hook is handed the BenchmarkRunner's JSON report
  // as an argument. Thus, it can add own metrics. Note that the JSON report can be empty when no output is exported.
  virtual std::optional<PostBenchmarkHook> post_benchmark_hook();
};

}  // namespace hyrise
