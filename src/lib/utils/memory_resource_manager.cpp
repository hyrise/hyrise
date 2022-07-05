#include "utils/memory_resource_manager.hpp"

namespace opossum {

const tbb::concurrent_vector<TrackedResource>& MemoryResourceManager::memory_resources() const {
  return _memory_resources;
}

std::shared_ptr<TrackingMemoryResource> MemoryResourceManager::get_memory_resource(const OperatorType operator_type, const std::string& operator_data_structure) {
  auto resource_ptr = std::make_shared<TrackingMemoryResource>();
  auto tracked_resource = TrackedResource{operator_type, operator_data_structure, resource_ptr};
  _memory_resources.push_back(tracked_resource);
  return resource_ptr;
}

}  // namespace opossum