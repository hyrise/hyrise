#include "numa_memory_resource.hpp"

#include <numa.h>
#include <boost/container/pmr/memory_resource.hpp>

namespace hyrise {

NumaMemoryResource::NumaMemoryResource(const NodeID node_id)
    : _num_allocations(0), _num_deallocations(0), _sum_allocated_bytes(0), _node_id(node_id) {
  _lap_num_allocations = 0;
  buffer = (char*)numa_alloc_onnode(ALLOCATED_BYTES, node_id);
}

void* NumaMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  _lap_num_allocations++;
  auto align_bytes = size_t{0};
  auto offset = size_t{0};
  if ((align_bytes = (bytes % alignment)) > 0) {
    bytes += alignment - align_bytes;
  }
  if (((offset = __sync_fetch_and_add(&_sum_allocated_bytes, bytes))) > ALLOCATED_BYTES) {
    std::cout << "Out of allocated memory on node" << _node_id << std::endl;
    return (nullptr);
  } else {
    return (void*)(buffer + offset);
  }
  return (nullptr);
}

void NumaMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
  // TODO: Free memory somewhere (Destructor of NumaMemoryResource)
  _num_deallocations++;
}

bool NumaMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

size_t NumaMemoryResource::lap_num_allocations() {
  _num_allocations += _lap_num_allocations;
  size_t tmp = _lap_num_allocations;
  _lap_num_allocations = 0;
  return tmp;
}

}  // namespace hyrise
