#include "numa_memory_resource.hpp"

#include <numa.h>
#include <boost/container/pmr/memory_resource.hpp>

namespace hyrise {

NumaMemoryResource::NumaMemoryResource(const NodeID node_id) : _num_allocations(0), _num_deallocations(0), _sum_allocated_bytes(0), _node_id(node_id) {}

void* NumaMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  _num_allocations++;
  _sum_allocated_bytes += bytes; 
  return numa_alloc_onnode(bytes, _node_id);
}

void NumaMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
  numa_free(pointer, bytes);
  _num_deallocations++; 
}

bool NumaMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise
