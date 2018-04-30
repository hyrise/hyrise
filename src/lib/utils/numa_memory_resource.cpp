#include "numa_memory_resource.hpp"

#include <boost/math/common_factor_rt.hpp>
#include <string>

#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#define NUMA_MEMORY_RESOURCE_ARENA_SIZE 1LL << 30
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

void NUMAMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) { _memory_source.free(p); }

int NUMAMemoryResource::get_node_id() const { return _memory_source.getPhysicalNode(); }

#else

NUMAMemoryResource::NUMAMemoryResource(int node_id, const std::string& name) {}

void* NUMAMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return boost::container::pmr::get_default_resource()->allocate(bytes, alignment);
}

void NUMAMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  boost::container::pmr::get_default_resource()->deallocate(p, bytes, alignment);
}

int NUMAMemoryResource::get_node_id() const { return UNDEFINED_NODE_ID; }
#endif

bool NUMAMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  // "Two memory_resources compare equal if and only if memory allocated from one memory_resource can be deallocated
  // from the other and vice versa."
  //
  // This function is problematic if we compare the behavior with and without HYRISE_NUMA_SUPPORT:
  //
  // auto resource_a = NUMAMemoryResource(numa_node, "foo");
  // auto resource_b = NUMAMemoryResource(numa_node, "bar");
  // resource_a.is_equal(resource_b); // false if with NUMA support, true if without
  //
  // This is not used right now and I am not sure in what context this might be needed. Let's prevent any surprises
  // if someone decides to use this in the future:

  Fail("NUMAMemoryResource::do_is_equal is not implemented - see comment");
}

}  // namespace opossum
