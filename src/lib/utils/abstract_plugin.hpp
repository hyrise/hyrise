#pragma once

#include <iostream>

#include "types.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

// This is necessary to make the plugin instantiable, it leads to plain C linkage to avoid
// ugly mangled names. Use EXPORT in the implementation file of your plugin.
#define EXPORT_PLUGIN(PluginName)        \
  extern "C" AbstractPlugin* factory() { \
    return new PluginName();             \
  }

using PluginFunctionName = std::string;
using PluginFunctionPointer = std::function<void(void)>;

// AbstractPlugin is the abstract super class for all plugins. An example implementation can be found
// under test/utils/test_plugin.cpp. Usually plugins are implemented as singletons because there
// shouldn't be multiple instances of them as they would compete against each other.
class AbstractPlugin {
 public:
  virtual ~AbstractPlugin() = default;

  virtual std::string description() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;

  // This method provides an interface for making plugin functions executable by plugin users.
  // Please see the test_plugin.cpp and the meta_exec_table_test.cpp for examples on how to use it.
  // IMPORTANT: The execution of such user-executable functions is blocking, see the PluginManager's
  // exec_user_function method. If you are writing a plugin and provide user-exectuable functions it
  // is YOUR responsibility to keep these function calls as short and efficient as possible, e.g.,
  // by spinning up a thread inside the plugin to execute the actual functionality.
  virtual std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions();
};

}  // namespace hyrise
