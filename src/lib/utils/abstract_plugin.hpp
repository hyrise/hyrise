#pragma once

#include <iostream>

#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// This is necessary to make the plugin instantiable, it leads to plain C linkage to avoid
// ugly mangled names. Use EXPORT in the implementation file of your plugin.
#define EXPORT_PLUGIN(PluginName)         \
  extern "C" AbstractPlugin* factory() {  \
    return new PluginName();              \
  }

// AbstractPlugin is the abstract super class for all plugins. An example implementation can be found
// under test/utils/test_plugin.cpp. Usually plugins are implemented as singletons because there
// shouldn't be multiple instances of them as they would compete against each other.
class AbstractPlugin {
 public:
  virtual ~AbstractPlugin() = default;

  virtual const std::string description() const = 0;

  virtual void start() = 0;

  virtual void stop() = 0;
};

}  // namespace opossum
