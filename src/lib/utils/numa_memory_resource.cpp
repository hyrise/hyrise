#include "numa_memory_resource.hpp"

#include <boost/math/common_factor_rt.hpp>
#include <string>

#if HYRISE_NUMA_SUPPORT
#define NUMA_MEMORY_RESOURCE_ARENA_SIZE 1llu << 30u
#else
#include <boost/container/pmr/global_resource.hpp>
#endif

namespace opossum {

#if HYRISE_NUMA_SUPPORT
NUMAMemoryResource::NUMAMemoryResource(int node_id, const std::string& name)
    : _memory_source(numa::MemSource::create(node_id, NUMA_MEMORY_RESOURCE_ARENA_SIZE, name.c_str())) {}

void* NUMAMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return _memory_source.allocAligned(boost::math::lcm(_alignment, alignment), bytes);
}

void NUMAMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) { numa::MemSource::free(p); }

bool NUMAMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  const auto other_numa_resource = dynamic_cast<const NUMAMemoryResource*>(&other);
  if (other_numa_resource == nullptr) {
    return false;
  } else {
    return other_numa_resource->_memory_source == _memory_source;
  }
}

int NUMAMemoryResource::get_node_id() const { return _memory_source.getPhysicalNode(); }

#else

NUMAMemoryResource::NUMAMemoryResource(int node_id, const std::string& name) {}

void* NUMAMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return boost::container::pmr::get_default_resource()->allocate(bytes, alignment);
}

void NUMAMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  boost::container::pmr::get_default_resource()->deallocate(p, bytes, alignment);
}

bool NUMAMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return true; }

int NUMAMemoryResource::get_node_id() const { return UNDEFINED_NODE_ID; }
#endif

}  // namespace opossum
