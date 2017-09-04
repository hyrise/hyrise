#include "numa_memory_resource.hpp"

#include <boost/math/common_factor_rt.hpp>

namespace opossum {

NUMAMemoryResource::NUMAMemoryResource(int node_id, size_t arena_size, const char* name, size_t mmap_threshold)
    : _mem_source(numa::MemSource::create(node_id, arena_size, name, mmap_threshold)) {}

void* NUMAMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return _mem_source.allocAligned(bytes, boost::math::lcm(_alignment, alignment));
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

static NUMAMemoryResource default_resource = NUMAMemoryResource(1, 1LL << 30, "default", 1LL << 24);

NUMAMemoryResource* NUMAMemoryResource::get_default_resource() { return &default_resource; }
}