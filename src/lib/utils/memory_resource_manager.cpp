#include "utils/memory_resource_manager.hpp"

namespace opossum {

const tbb::concurrent_vector<ResourceRecord>& MemoryResourceManager::memory_resources() const {
  return _memory_resources;
}

boost::container::pmr::memory_resource* MemoryResourceManager::get_memory_resource(
    const OperatorType operator_type, const std::string& operator_data_structure) {
  // Return default memory resource if tracking is disabled.
  if (!_tracking_is_enabled) {
    return boost::container::pmr::get_default_resource();
  }

  auto resource_ptr = new TrackingMemoryResource();
  auto tracked_resource = ResourceRecord{operator_type, operator_data_structure, resource_ptr};
  _memory_resources.push_back(tracked_resource);
  return resource_ptr;
}

void MemoryResourceManager::enable() {
  _tracking_is_enabled = true;
}

void MemoryResourceManager::disable() {
  _tracking_is_enabled = false;
}

}  // namespace opossum
