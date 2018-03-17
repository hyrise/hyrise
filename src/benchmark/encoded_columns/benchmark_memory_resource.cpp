#include "benchmark_memory_resource.hpp"

#include <iostream>

namespace opossum {

BenchmarkMemoryResource::BenchmarkMemoryResource(int node_id)
    : NUMAMemoryResource{node_id, "Benchmark Memory Resource"}, _currently_allocated{0u} {}

void* BenchmarkMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  _currently_allocated += bytes;
  // std::cout << "Currently Allocated (+" << bytes << "): " << _currently_allocated << std::endl;
  return NUMAMemoryResource::do_allocate(bytes, alignment);
}

void BenchmarkMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  _currently_allocated -= bytes;
  // std::cout << "Currently Allocated (-" << bytes << "): " << _currently_allocated << std::endl;
  NUMAMemoryResource::do_deallocate(p, bytes, alignment);
}

bool BenchmarkMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  auto casted_resource = dynamic_cast<const BenchmarkMemoryResource*>(&other);
  return (casted_resource != nullptr) && (_currently_allocated == casted_resource->_currently_allocated)
         && NUMAMemoryResource::do_is_equal(other);
}

std::size_t BenchmarkMemoryResource::currently_allocated() const {
  return _currently_allocated;
}

}  // namespace opossum
