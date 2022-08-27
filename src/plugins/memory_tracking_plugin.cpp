#include "memory_tracking_plugin.hpp"

namespace opossum {

std::string MemoryTrackingPlugin::description() const {
  return "Activate and deactivate the tracking of temporary memory usage. Cleanup memory usage data.";
}

void MemoryTrackingPlugin::start() {}

void MemoryTrackingPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> MemoryTrackingPlugin::provided_user_executable_functions()
    const {
  return {
    {"is_enabled", [&]() { this->is_enabled(); }},
    {"enable", [&]() { this->enable(); }},
    {"disable", [&]() { this->disable(); }},
    {"cleanup", [&]() { this->cleanup(); }}
  };
}

bool MemoryTrackingPlugin::is_enabled() const { 
  return _memory_resource_manager.tracking_is_enabled();
}

void MemoryTrackingPlugin::enable() const {
  _memory_resource_manager.enable_temporary_memory_tracking();
}

void MemoryTrackingPlugin::disable() const {
  _memory_resource_manager.disable_temporary_memory_tracking();
  cleanup();
}

void MemoryTrackingPlugin::cleanup() const {
  // Calling clear on the vector has the effect that the desctructor is called for each ResourceRecord. This 
  // invalidates the unique pointer to the TrackingMemoryResource stored there. Thus, the TrackingMemoryResource and
  // its stored data are freed.
  // TODO: confirm if this actually works.
  _memory_resource_manager._memory_resources.clear();
  _memory_resource_manager._memory_resources.shrink_to_fit();
}

EXPORT_PLUGIN(MemoryTrackingPlugin)

}  // namespace opossum
