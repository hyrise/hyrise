#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/meta_tables/abstract_meta_table.hpp"
#include "memory/tracking_memory_resource.hpp"

namespace opossum {

class MemoryResourceManager : public Noncopyable {
 public:

  // TODO comment
  const std::unordered_map<std::string, std::shared_ptr<TrackingMemoryResource>>& memory_resources() const;
  const std::unordered_map<std::string, size_t> get_current_memory_usage() const;
  PolymorphicAllocator<std::byte> get_pmr_allocator(const std::string& purpose);
  std::shared_ptr<TrackingMemoryResource> get_memory_resource(const std::string& purpose);

 protected:
  // make sure that only Hyrise can create new instances
  friend class Hyrise;
  MemoryResourceManager() = default;

  std::unordered_map<std::string, std::shared_ptr<TrackingMemoryResource>> _memory_resources;
};

}  // namespace opossum