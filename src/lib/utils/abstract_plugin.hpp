#pragma once

#include <iostream>

#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class HyriseEnvironmentRef;

// This is necessary to make the plugin instantiable, it leads to plain C linkage to avoid
// ugly mangled names. Use EXPORT in the implementation file of your plugin.
#define EXPORT_PLUGIN(PluginName)                                                               \
  extern "C" AbstractPlugin* factory(const std::shared_ptr<HyriseEnvironmentRef>& hyrise_env) { \
    return new PluginName(hyrise_env);                                                          \
  }

// AbstractPlugin is the abstract super class for all plugins. An example implementation can be found
// under test/utils/test_plugin.cpp. Usually plugins are implemented as singletons because there
// shouldn't be multiple instances of them as they would compete against each other.
class AbstractPlugin {
 public:
  AbstractPlugin(const std::shared_ptr<HyriseEnvironmentRef>& init_hyrise_env);

  virtual ~AbstractPlugin() = default;

  virtual std::string description() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;

 protected:
  // Raw pointer is used since storage manager is the owner of plugin manager
  std::shared_ptr<HyriseEnvironmentRef> _hyrise_env;
};

}  // namespace opossum
