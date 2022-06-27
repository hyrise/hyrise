#include "utils/memory_resource_manager.hpp"

namespace opossum {

const std::unordered_map<std::string, std::shared_ptr<TrackingMemoryResource>>&
MemoryResourceManager::memory_resources() const {
  return _memory_resources;
}

std::shared_ptr<TrackingMemoryResource> MemoryResourceManager::get_memory_resource(const std::string& purpose) {
  // TODO: think about concurrency
  if (!_memory_resources.contains(purpose)) {
    _memory_resources.emplace(purpose, std::make_shared<TrackingMemoryResource>());
  }
  return _memory_resources[purpose];
}

PolymorphicAllocator<std::byte> MemoryResourceManager::get_pmr_allocator(const std::string& purpose) {
  // TODO: remove &(*..) hack (used to convert smart pointer to normal pointer)
  return PolymorphicAllocator<std::byte>{&(*get_memory_resource(purpose))};
}

}  // namespace opossum