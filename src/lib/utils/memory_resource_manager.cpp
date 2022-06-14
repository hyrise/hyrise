namespace opossum {

  const std::shared_ptr<std::shared_ptr<TrackinMemoryResource>>& MemoryResourceManager::memory_resources() const {
    return _memory_resources;
  }

  const std::unordered_map<std::string, uint_64> MemoryResourceManager::get_current_memory_usage() const {
    auto memory_stats = std::unordered_map<std::string, uint64_t>{};
    for (const auto& [purpose, memory_resource_ptr] : _memory_resources) {
      memory_stats[purpose] = memory_resource_ptr->get_amount();
    }
  }

  std::shared_ptr<TrackingMemoryResource> MemoryResourceManager::get_memory_resource(const std::string& purpose) {
    if (!_memory_resources.contains(purpose)) {
      _memory_resources[purpose] = std::make_shared<TrackingMemoryResource>{};
    }    
    return _memory_resources[purpose];
  }

}  // namespace opossum