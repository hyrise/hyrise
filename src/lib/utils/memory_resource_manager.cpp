#include "utils/memory_resource_manager.hpp"

namespace opossum {

const std::vector<TrackedResource>& MemoryResourceManager::memory_resources() const {
  return _memory_resources;
}

std::shared_ptr<TrackingMemoryResource> MemoryResourceManager::get_memory_resource(const OperatorType operator_type, const std::string& operator_data_structure) {
  auto resource_ptr = std::make_shared<TrackingMemoryResource>();
  _memory_resources.emplace_back(operator_type, operator_data_structure, resource_ptr);
  return resource_ptr;
}

}  // namespace opossum