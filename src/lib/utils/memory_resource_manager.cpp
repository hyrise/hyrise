#include "utils/memory_resource_manager.hpp"

namespace opossum {

const std::map<std::pair<OperatorType, std::string>, std::shared_ptr<TrackingMemoryResource>>& MemoryResourceManager::memory_resources() const {
  return _memory_resources;
}

std::shared_ptr<TrackingMemoryResource> MemoryResourceManager::get_memory_resource(const OperatorType operator_type, const std::string& operator_data_structure) {
  // TODO: think about concurrency
  const auto key = std::make_pair(operator_type, operator_data_structure);
  if (!_memory_resources.contains(key)) {
    _memory_resources.emplace(key, std::make_shared<TrackingMemoryResource>());
  }
  return _memory_resources[key];
}

}  // namespace opossum