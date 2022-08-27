#pragma once

#include "hyrise.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class MemoryTrackingPlugin : public AbstractPlugin {
 public:
  MemoryTrackingPlugin() : _memory_resource_manager(Hyrise::get().memory_resource_manager) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() const final;

  bool is_enabled() const;
  void enable() const;
  void disable() const;
  void cleanup() const;

 protected:
  MemoryResourceManager& _memory_resource_manager;
};

}  // namespace opossum
