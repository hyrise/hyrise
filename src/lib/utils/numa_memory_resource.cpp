#include "numa_memory_resource.hpp"

#include <boost/math/common_factor_rt.hpp>
#include <string>

#if OPOSSUM_NUMA_SUPPORT
#define NUMA_MEMORY_RESOURCE_ARENA_SIZE 1LL << 30
#else
#include <boost/container/pmr/global_resource.hpp>
#endif

namespace opossum {

#if OPOSSUM_NUMA_SUPPORT
NUMAMemoryResource::NUMAMemoryResource(int node_id, const std::string &name)
    : _mem_source(numa::MemSource::create(node_id, NUMA_MEMORY_RESOURCE_ARENA_SIZE, name.c_str())) {}

void* NUMAMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return _mem_source.allocAligned(boost::math::lcm(_alignment, alignment), bytes);
}

void NUMAMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) { _mem_source.free(p); }

bool NUMAMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  const auto other_numa_resource = dynamic_cast<const NUMAMemoryResource*>(&other);
  if (other_numa_resource == NULL) {
    return false;
  } else {
    return other_numa_resource->_mem_source == _mem_source;
  }
}

#else

NUMAMemoryResource::NUMAMemoryResource(int node_id, const std::string &name) {}

void* NUMAMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return boost::container::pmr::get_default_resource()->allocate(bytes, alignment);
}

void NUMAMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  boost::container::pmr::get_default_resource()->deallocate(p, bytes, alignment);
}

bool NUMAMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return true; }
#endif

static NUMAMemoryResource default_resource = NUMAMemoryResource(1, "default");

NUMAMemoryResource* NUMAMemoryResource::get_default_resource() { return &default_resource; }
}  // namespace opossum
