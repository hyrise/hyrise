#pragma once

#include <iostream>

#include "storage/storage_manager.hpp"
#include "utils/singleton.hpp"
#include "types.hpp"

namespace opossum {

#define EXPORT(PluginName)                                            \
  extern "C" AbstractPlugin* factory() {                              \
    auto plugin = static_cast<AbstractPlugin*>(&(PluginName::get())); \
    return plugin;                                                    \
  }

class AbstractPlugin {
 public:
  virtual const std::string description() const = 0;

  virtual void start() const = 0;

  virtual void stop() const = 0;
};

}  // namespace opossum
