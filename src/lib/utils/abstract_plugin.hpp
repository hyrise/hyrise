#pragma once

#include <iostream>

#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// This is necessary to make the plugin instantiable, it leads to plain C linkage to avoid
// ugly mangled names. Use EXPORT in the implementation file of your plugin.
#define EXPORT_PLUGIN(PluginName) \
  extern "C" AbstractPlugin* factory() { return new PluginName(); }

// This macro allows the function `FunctionName` of the plugin called `PluginName` to be
// externally callable. This is necessary to enable user-callable functions. As above,
// it leads to plain C linkage. Use EXPORT_USER_CALLABLE_FUNCTION in the implementation
// file of your plugin if you want to make a function user-callable.
#define EXPORT_USER_CALLABLE_FUNCTION(PluginName, FunctionName) \
  /* Careful! We are getting a raw pointer from a smart (unique) pointer here. */ \
  /* This could conflict with the intention of unique_ptr. It should not be    */ \
  /* a problem, as long as we handle it responsible and let it go out of scope */ \
  /* at the end of this function.                                              */ \
  extern "C" void FunctionName ## _extern(const std::unique_ptr<AbstractPlugin>& plugin) { \
    const auto plugin_raw = plugin.get(); \
    const auto test_plugin = static_cast<const PluginName*>(plugin_raw); \
    Assert(test_plugin, "Couldn't cast to correct plugin type. Nullptr provided?"); \
    test_plugin->FunctionName(); \
  }

// AbstractPlugin is the abstract super class for all plugins. An example implementation can be found
// under test/utils/test_plugin.cpp. Usually plugins are implemented as singletons because there
// shouldn't be multiple instances of them as they would compete against each other.
class AbstractPlugin {
 public:
  virtual ~AbstractPlugin() = default;

  virtual std::string description() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;
};

}  // namespace opossum
